package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import java.nio.file.{Files, Path, Paths}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

/** Source-level quality gates.
  *
  * The gates, enforced over every .scala file under src/:
  *   - no `var`    — mutable local state is imperative; use vals, folds,
  *                   recursion, or concurrent containers (Ref/Atomic*)
  *   - no `while`  — iteration is fold/recurse territory
  *   - no `return` — early exits are if/else and match expressions
  *   - no function over 80 lines — decompose monster functions into helpers
  *
  * Scanning strips comments and string/char literals first, so `return` inside
  * an embedded JavaScript snippet (TraceServer's HTML builder) or the word
  * `while` in a docstring never trips the gate. Code inside a `${...}` slot of
  * an interpolated string is stripped too — a known blind spot, same as any
  * literal-level scanner. The scanner is written declaratively, so it passes
  * the gate it enforces. */
object CodeQualityTests extends zio.test.ZIOSpecDefault {

  private def sourceFiles: List[Path] = {
    val roots = Seq("src/main/scala", "src/test/scala")
    roots.flatMap { root =>
      val p = Paths.get(root)
      if (Files.isDirectory(p))
        Files.walk(p).iterator().asScala
          .filter(f => f.toString.endsWith(".scala") && Files.isRegularFile(f))
          .toList
      else Nil
    }.toList
  }

  private def blank(c: Char): Char = if (c == '\n') '\n' else ' '

  /** Replace comments and string/char literals with spaces, keeping newlines so
    * line numbers survive. States: code / line comment / block comment /
    * string / triple-quoted string / char literal. */
  private def stripLiterals(code: String): String = {
    val out = new StringBuilder(code.length)
    @tailrec def go(i: Int, state: String): Unit =
      if (i >= code.length) ()
      else {
        val c    = code(i)
        val next = if (i + 1 < code.length) code(i + 1) else ' '
        val d3   = if (i + 2 < code.length) code(i + 2) else ' '
        state match {
          case "code" =>
            if (c == '/' && next == '/')      { out.append("  "); go(i + 2, "line") }
            else if (c == '/' && next == '*') { out.append("  "); go(i + 2, "block") }
            else if (c == '"' && next == '"' && d3 == '"') { out.append("   "); go(i + 3, "triple") }
            else if (c == '"')                { out.append(' '); go(i + 1, "str") }
            else if (c == '\'')               { out.append(' '); go(i + 1, "char") }
            else                              { out.append(c); go(i + 1, state) }
          case "line" =>
            if (c == '\n') { out.append(c); go(i + 1, "code") } else { out.append(' '); go(i + 1, state) }
          case "block" =>
            if (c == '*' && next == '/')      { out.append("  "); go(i + 2, "code") }
            else                              { out.append(blank(c)); go(i + 1, state) }
          case "str" =>
            val advance = if (c == '\\' && i + 1 < code.length) 2 else 1
            val nextState = if (advance == 1 && c == '"') "code" else state
            out.append(blank(c))
            if (advance == 2) out.append(blank(next))
            go(i + advance, nextState)
          case "triple" =>
            if (c == '"' && next == '"' && d3 == '"') { out.append("   "); go(i + 3, "code") }
            else { out.append(blank(c)); go(i + 1, state) }
          case "char" =>
            val advance = if (c == '\\' && i + 1 < code.length) 2 else 1
            val nextState = if (advance == 1 && c == '\'') "code" else state
            out.append(blank(c))
            if (advance == 2) out.append(blank(next))
            go(i + advance, nextState)
        }
      }
    go(0, "code")
    out.toString
  }

  private val defRe: Regex    = """\bdef\s+([A-Za-z0-9_]+)""".r
  private val bannedRe: Regex = """\b(var|while|return)\b""".r

  private case class OpenDef(name: String, startLine: Int, depthBefore: Int, indent: Int, braced: Boolean)
  private type Scan = (List[(String, Int, Int)], List[OpenDef], Int)

  /** (name, startLine 1-based, length in lines) for every def in stripped source.
    *
    * A def's body is tracked in one of two modes. Braced bodies (opened on the
    * def line or any later line) end where brace depth falls back to the depth
    * the def started at. Brace-less bodies end at the last line before the
    * first non-blank line indented no deeper than the def itself. */
  private def functionSpans(stripped: String): List[(String, Int, Int)] = {
    val (closedRev, _, _) = stripped.split("\n").zipWithIndex.foldLeft[(Scan)]((Nil, Nil, 0)) {
      case ((closed, stack, depth), (line, idx)) =>
        val lineNo = idx + 1
        val stackWithNew = defRe.findFirstMatchIn(line).map { m =>
          val bodyOpen = line.substring(m.end).contains("{")
          OpenDef(m.group(1), lineNo, depth, line.takeWhile(_ == ' ').length, bodyOpen) :: stack
        }.getOrElse(stack)
        val opens  = line.count(_ == '{')
        val closes = line.count(_ == '}')
        val depth2 = depth + opens - closes
        val stack2 =
          if (opens > closes) stackWithNew.map(d => if (lineNo > d.startLine) d.copy(braced = true) else d)
          else stackWithNew
        val (closed2, stack3) = stack2.foldLeft((closed, List.empty[OpenDef])) {
          case ((acc, open), d) =>
            val (endLine, done) =
              if (d.braced) (lineNo, depth2 <= d.depthBefore)
              else {
                val bodyIndent = line.takeWhile(_ == ' ').length
                val ended = lineNo > d.startLine && line.trim.nonEmpty && bodyIndent <= d.indent
                (lineNo - 1, ended)
              }
            if (done) (((d.name, d.startLine, endLine - d.startLine + 1) :: acc), open)
            else (acc, d :: open)
        }
        (closed2, stack3.reverse, depth2)
    }
    closedRev.reverse
  }

  private case class Violation(file: String, line: Int, rule: String, code: String)

  private def scanFile(path: Path): (List[Violation], List[Violation]) = {
    val stripped = stripLiterals(Files.readString(path))
    val name = path.toString.replace('\\', '/')
    val banned = stripped.split("\n").zipWithIndex.collect {
      case (line, idx) if bannedRe.findFirstIn(line).isDefined =>
        val rules = bannedRe.findAllMatchIn(line).map(_.group(1)).mkString("+")
        Violation(name, idx + 1, rules, line.trim.take(90))
    }.toList
    val monsters = functionSpans(stripped)
      .collect { case (fn, start, len) if len > MaxFunctionLines =>
        Violation(name, start, s"${len}L", s"def $fn exceeds $MaxFunctionLines lines")
      }
    (banned, monsters)
  }

  private val MaxFunctionLines = 80

  private def render(vs: List[Violation]): String =
    vs.map(v => s"${v.file}:${v.line} [${v.rule}] ${v.code}").mkString("\n")

  private lazy val (bannedViolations, monsterViolations): (List[Violation], List[Violation]) =
    sourceFiles.map(scanFile).foldLeft((List.empty[Violation], List.empty[Violation])) {
      case ((banned, monsters), (fileBanned, fileMonsters)) => (banned ++ fileBanned, monsters ++ fileMonsters)
    }

  def spec = suite("CodeQualityTests")(
    test("scanner actually sees the source tree (not vacuously green)") {
      assert(sourceFiles.length)(Assertion.isGreaterThanEqualTo(30))
    },
    test("no imperative constructs: var, while, return") {
      assert(render(bannedViolations))(Assertion.equalTo(""))
    },
    test(s"no monster functions over $MaxFunctionLines lines") {
      assert(render(monsterViolations))(Assertion.equalTo(""))
    }
  )
}
