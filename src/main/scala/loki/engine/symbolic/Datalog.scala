package loki.engine.symbolic

import scala.annotation.tailrec
import scala.collection.mutable

/** Error types for the Datalog engine. */
class DatalogError(msg: String) extends Exception(msg)
class ParseError(msg: String) extends DatalogError(msg)
class UnsafeRuleError(msg: String) extends DatalogError(msg)
class StratificationError(msg: String) extends DatalogError(msg)

/** A ground fact: List(predicate, arg1, arg2, ...) */
type GroundFact = List[Any]

/** Logic variable, e.g. ?x */
case class Var(name: String)

/** Constant term (str, int, double, boolean) */
case class Const(value: Any)

/** A term is either a variable or a constant. */
type Term = Var | Const

/** A predicate applied to terms. */
case class Atom(predicate: String, terms: Vector[Term] = Vector.empty, negated: Boolean = false) {
  def arity: Int = terms.length
  def vars: Set[String] = terms.collect { case Var(n) => n }.toSet
}

/** Body literal: either an atom or a comparison built-in. */
sealed trait BodyLiteral {
  def vars: Set[String]
}
case class AtomLit(atom: Atom) extends BodyLiteral {
  def vars: Set[String] = atom.vars
}
case class Comparison(op: String, left: Term, right: Term) extends BodyLiteral {
  def vars: Set[String] = List(left, right).collect { case Var(n) => n }.toSet
}

/** A Horn clause: head :- body. A fact has an empty body. */
case class Rule(head: Atom, body: Vector[BodyLiteral] = Vector.empty, name: String = "") {
  def isFact: Boolean = body.isEmpty
}

/** Result of running a Program. */
case class Solution(
  facts: Set[GroundFact],
  derived: Set[GroundFact],
  justifications: Map[GroundFact, (String, List[GroundFact])] = Map.empty
) {
  def query(predicate: String): List[GroundFact] =
    facts.filter(_.headOption.exists(_ == predicate)).toList.sortBy(_.toString)

  def explain(f: GroundFact): String = justifications.get(f) match {
    case Some((ruleName, support)) =>
      val because = if (support.isEmpty) "(no premises)"
                    else support.map(factToDict(_).toString).mkString(", ")
      s"${f.headOption.getOrElse("")}${f.tail} ⟸ $because   [rule: $ruleName]"
    case None => s"$f (given)"
  }
}

// ---------------------------------------------------------------------------
// Fact helpers
// ---------------------------------------------------------------------------

def fact(predicate: String, args: Any*): GroundFact = predicate :: args.toList

def factToDict(f: GroundFact): Map[String, Any] =
  Map("predicate" -> f.headOption.getOrElse(""), "args" -> f.tail)

def normaliseFact(item: Any): GroundFact = item match {
  case list: List[_] if list.nonEmpty  => list.asInstanceOf[List[Any]]
  case map: Map[_, _] =>
    val m = map.asInstanceOf[Map[String, Any]]
    val pred = m.getOrElse("predicate", m.getOrElse("pred",
      throw new DatalogError(s"fact dict missing 'predicate': $m")))
    val args = m.getOrElse("args", m.getOrElse("terms", Nil)).asInstanceOf[List[Any]]
    pred :: args
  case s: String =>
    val atom = DatalogParser.parseAtom(s)
    if (atom.vars.nonEmpty) throw new UnsafeRuleError(s"input fact contains variables: $s")
    atom.predicate :: atom.terms.map {
      case Const(v) => v
      case Var(n)   => throw new UnsafeRuleError(s"variable ?$n in fact")
    }.toList
  case _ => throw new DatalogError(s"cannot interpret fact: $item")
}

def atomsToFacts(items: Iterable[Any]): Set[GroundFact] = items.map(normaliseFact).toSet

def parseProgram(text: String): List[Rule] = DatalogParser.parseProgram(text)

def ruleFromObj(obj: Any): List[Rule] = obj match {
  case r: Rule   => List(r)
  case s: String => DatalogParser.parseProgram(s)
  case m: Map[_, _] =>
    val map = m.asInstanceOf[Map[String, Any]]
    val headAtom: Atom = map.getOrElse("head", throw new ParseError(s"rule dict missing 'head': $map")) match {
      case s: String     => DatalogParser.parseAtom(s)
      case hm: Map[_, _] =>
        val hmap  = hm.asInstanceOf[Map[String, Any]]
        val pred  = hmap.getOrElse("predicate", hmap.getOrElse("pred", throw new ParseError("head map missing 'predicate'"))).toString
        val terms = hmap.getOrElse("terms", hmap.getOrElse("args", Nil)) match {
          case l: List[_] => l.map(t => DatalogParser.parseTerm(t.toString)).toVector
          case _          => Vector.empty
        }
        Atom(pred, terms)
      case other => throw new ParseError(s"cannot parse head: $other")
    }
    val bodyLiterals: Vector[BodyLiteral] = map.getOrElse("body", Nil) match {
      case s: String  => DatalogParser.splitTopLevel(s).filter(_.trim.nonEmpty).map(DatalogParser.parseBodyLiteral).toVector
      case l: List[_] => l.flatMap {
        case s: String     => List(DatalogParser.parseBodyLiteral(s))
        case bm: Map[_, _] =>
          val bmap     = bm.asInstanceOf[Map[String, Any]]
          val pred     = bmap.getOrElse("predicate", bmap.getOrElse("pred", throw new ParseError("body map missing 'predicate'"))).toString
          val negated  = bmap.get("negated").exists(_ == true)
          val terms    = bmap.getOrElse("terms", bmap.getOrElse("args", Nil)) match {
            case l2: List[_] => l2.map(t => DatalogParser.parseTerm(t.toString)).toVector
            case _           => Vector.empty
          }
          List(AtomLit(Atom(pred, terms, negated)))
        case other => List(DatalogParser.parseBodyLiteral(other.toString))
      }.toVector
      case _ => Vector.empty
    }
    List(Rule(headAtom, bodyLiterals, headAtom.predicate))
  case _ => throw new ParseError(s"cannot interpret rule: $obj")
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

class Program(val rules: List[Rule]) {

  private val baseFacts: Set[GroundFact] = rules.filter(_.isFact).map { r =>
    if (r.head.vars.nonEmpty)
      throw new UnsafeRuleError(s"fact has unbound variables: $r")
    r.head.predicate :: r.head.terms.map {
      case Const(v) => v
      case Var(n)   => throw new UnsafeRuleError(s"variable ?$n in fact head")
    }.toList
  }.toSet

  private val derivationRules: List[Rule] =
    rules.filterNot(_.isFact).map(compileRule)

  private val intensional: Set[String] =
    derivationRules.map(_.head.predicate).toSet

  val strata: List[Set[String]] = stratify(derivationRules, intensional)

  def solve(inputFacts: Iterable[Any] = Nil, withProof: Boolean = false): Solution = {
    val db0: Set[GroundFact] = baseFacts ++ inputFacts.map(normaliseFact).toSet
    val inputFactSet = db0

    def round(stratumRules: List[Rule], db: Set[GroundFact], just: Map[GroundFact, (String, List[GroundFact])])
        : (Set[GroundFact], Map[GroundFact, (String, List[GroundFact])]) = {
      val (newFacts, just2) = stratumRules.foldLeft((Set.empty[GroundFact], just)) { case (acc, rule) =>
        matchBody(rule.body, db).foldLeft(acc) { case ((facts, j), (binding, support)) =>
          val newFact = groundAtom(rule.head, binding)
          if (!db.contains(newFact) && !facts.contains(newFact)) {
            val withJust =
              if (withProof && !j.contains(newFact)) j + (newFact -> (rule.name.take(200), support.reverse)) else j
            (facts + newFact, withJust)
          } else (facts, j)
        }
      }
      if (newFacts.isEmpty) (db, just2) else round(stratumRules, db ++ newFacts, just2)
    }

    val (db, justifications) = strata.foldLeft((db0, Map.empty[GroundFact, (String, List[GroundFact])])) {
      case ((acc, just), stratum) =>
        val stratumRules = derivationRules.filter(r => stratum.contains(r.head.predicate))
        round(stratumRules, acc, just)
    }

    Solution(db, db -- inputFactSet, justifications)
  }
}

object Program {
  def parse(text: String): Program = new Program(DatalogParser.parseProgram(text))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

private def compileRule(rule: Rule): Rule = {
  val positives = rule.body.collect { case a: AtomLit if !a.atom.negated => a }
  val others    = rule.body.filter  { case a: AtomLit if !a.atom.negated => false; case _ => true }
  val posVars   = positives.flatMap(_.vars).toSet

  val unboundHead = rule.head.vars -- posVars
  if (unboundHead.nonEmpty)
    throw new UnsafeRuleError(
      s"unsafe rule ${rule.name.take(60)}: head variable(s) " +
      s"${unboundHead.map("?" + _).toList.sorted} not bound by any positive literal"
    )

  for (lit <- others) {
    val unbound = lit.vars -- posVars
    if (unbound.nonEmpty) {
      val kind = lit match { case _: AtomLit => "negated literal"; case _ => "comparison" }
      throw new UnsafeRuleError(
        s"unsafe rule ${rule.name.take(60)}: $kind has unbound variable(s) " +
        s"${unbound.map("?" + _).toList.sorted}"
      )
    }
  }

  Rule(rule.head, positives ++ others, rule.name)
}

private def stratify(rules: List[Rule], intensional: Set[String]): List[Set[String]] = {
  val maxIters = intensional.size + 1

  def demands(stratum: Map[String, Int]): Option[Map[String, Int]] = {
    val next = rules.flatMap { rule =>
      rule.body.collect {
        case AtomLit(atom) if intensional.contains(atom.predicate) =>
          rule.head.predicate -> (stratum(atom.predicate) + (if (atom.negated) 1 else 0))
      }
    }.foldLeft(stratum) { case (s, (head, required)) =>
      if (required > s(head)) s + (head -> required) else s
    }
    if (next == stratum) None else Some(next)
  }

  @tailrec def fixpoint(stratum: Map[String, Int], iter: Int): Map[String, Int] =
    demands(stratum) match {
      case None        => stratum
      case Some(next)  =>
        if (iter + 1 >= maxIters)
          throw new StratificationError(
            "program cannot be stratified: recursion through negation detected"
          )
        else fixpoint(next, iter + 1)
    }

  val stratum = fixpoint(intensional.map(_ -> 0).toMap, 0)

  for (rule <- rules; lit <- rule.body) {
    lit match {
      case AtomLit(atom) if atom.negated && intensional.contains(atom.predicate) =>
        if (stratum(atom.predicate) >= stratum(rule.head.predicate))
          throw new StratificationError(
            s"program cannot be stratified: negated ${atom.predicate} is recursively " +
            s"derived with ${rule.head.predicate}"
          )
      case _ =>
    }
  }

  stratum.groupBy(_._2).toList.sortBy(_._1).map(_._2.keySet.toSet)
}

private def matchBody(
  body: Vector[BodyLiteral],
  db:   Set[GroundFact]
): Iterator[(Map[String, Any], List[GroundFact])] = {

  def rec(
    i:       Int,
    binding: Map[String, Any],
    support: List[GroundFact]
  ): Iterator[(Map[String, Any], List[GroundFact])] =
    if (i == body.length) Iterator.single((binding, support))
    else body(i) match {
      case Comparison(op, left, right) =>
        if (evalComparison(op, left, right, binding)) rec(i + 1, binding, support)
        else Iterator.empty
      case AtomLit(atom) if atom.negated =>
        val ground = groundAtom(atom, binding)
        if (!db.contains(ground)) rec(i + 1, binding, support)
        else Iterator.empty
      case AtomLit(atom) =>
        db.iterator
          .filter(f => f.headOption.exists(_ == atom.predicate) && f.length - 1 == atom.arity)
          .flatMap { f =>
            unifyTerms(atom.terms, f.tail, binding) match {
              case Some(nb) => rec(i + 1, nb, f :: support)
              case None     => Iterator.empty
            }
          }
    }

  rec(0, Map.empty, Nil)
}

private def unifyTerms(
  terms:   Vector[Term],
  values:  List[Any],
  binding: Map[String, Any]
): Option[Map[String, Any]] =
  if (terms.length != values.length) None
  else terms.zip(values).foldLeft(Option(binding)) {
    case (None, _)                     => None
    case (Some(nb), (Const(v), value)) => if (v != value) None else Some(nb)
    case (Some(nb), (Var(n), value))   =>
      nb.get(n) match {
        case Some(existing) => if (existing != value) None else Some(nb)
        case None           => Some(nb + (n -> value))
      }
  }

private def groundAtom(atom: Atom, binding: Map[String, Any]): GroundFact =
  atom.predicate :: atom.terms.map {
    case Const(v) => v
    case Var(n)   => binding.getOrElse(n, throw new DatalogError(s"unbound variable ?$n"))
  }.toList

private def evalComparison(op: String, left: Term, right: Term, binding: Map[String, Any]): Boolean = {
  val l = resolveTerm(left, binding)
  val r = resolveTerm(right, binding)
  op match {
    case "=" | "==" => l == r
    case "!="       => l != r
    case "<" | "<=" | ">" | ">=" =>
      (toDouble(l), toDouble(r)) match {
        case (Some(a), Some(b)) => op match {
          case "<"  => a < b;  case "<=" => a <= b
          case ">"  => a > b;  case ">=" => a >= b
        }
        case _ => throw new DatalogError(s"cannot compare $l $op $r")
      }
    case _ => throw new DatalogError(s"unknown operator: $op")
  }
}

private def resolveTerm(term: Term, binding: Map[String, Any]): Any = term match {
  case Const(v) => v
  case Var(n)   => binding.getOrElse(n, throw new DatalogError(s"unbound variable ?$n"))
}

private def toDouble(v: Any): Option[Double] = v match {
  case n: Int    => Some(n.toDouble)
  case n: Long   => Some(n.toDouble)
  case n: Double => Some(n)
  case n: Float  => Some(n.toDouble)
  case s: String => s.toDoubleOption
  case _         => None
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

object DatalogParser {

  def parseProgram(text: String): List[Rule] = {
    val cleaned = stripComments(text)
    splitStatements(cleaned).map(_.trim).filter(_.nonEmpty).map { stmt =>
      val colonIdx = stmt.indexOf(":-")
      if (colonIdx >= 0) {
        val headSrc = stmt.take(colonIdx)
        val bodySrc = stmt.drop(colonIdx + 2)
        val head = parseAtom(headSrc)
        val body = splitTopLevel(bodySrc).filter(_.trim.nonEmpty)
                     .map(b => parseBodyLiteral(b)).toVector
        Rule(head, body, stmt.take(200))
      } else {
        Rule(parseAtom(stmt), Vector.empty, stmt.take(200))
      }
    }
  }

  private val varRe      = "^\\?[A-Za-z_][A-Za-z0-9_]*$".r
  private val upperVarRe = "^[A-Z_][A-Za-z0-9_]*$".r
  private val intRe      = "^[+-]?\\d+$".r
  private val floatRe    = "^[+-]?(\\d+\\.\\d*|\\.\\d+|\\d+)([eE][+-]?\\d+)?$".r
  private val atomRe     = "^([A-Za-z_][A-Za-z0-9_]*)\\s*\\((.*)\\)$".r

  def parseTerm(token: String): Term = {
    val t = token.trim
    if (t.isEmpty) throw new ParseError("empty term")
    if (varRe.matches(t)) Var(t.drop(1))          // ?X style
    else if (upperVarRe.matches(t)) Var(t)        // X style (uppercase / _wildcard)
    else if ((t.startsWith("\"") && t.endsWith("\"") && t.length >= 2) ||
             (t.startsWith("'")  && t.endsWith("'")  && t.length >= 2))
      Const(t.drop(1).dropRight(1))
    else intRe.findFirstIn(t).map(_ => Const(t.toInt))
      .orElse(floatRe.findFirstIn(t).map(_ => Const(t.toDouble)))
      .orElse(if (t == "true") Some(Const(true)) else None)
      .orElse(if (t == "false") Some(Const(false)) else None)
      .getOrElse(Const(t))
  }

  def parseAtom(text: String, negated: Boolean = false): Atom = {
    val t = text.trim
    atomRe.findFirstMatchIn(t) match {
      case Some(m) =>
        val predicate = m.group(1)
        val argsSrc   = m.group(2).trim
        val terms = if (argsSrc.isEmpty) Vector.empty
                    else splitTopLevel(argsSrc).map(s => parseTerm(s)).toVector
        Atom(predicate, terms, negated)
      case None =>
        throw new ParseError(s"malformed atom: '${t.take(60)}' (expected 'pred(args)')")
    }
  }

  private val comparators = List("<=", ">=", "==", "!=", "=", "<", ">")

  private def comparisonAt(t: String, op: String, idx: Int): Option[Comparison] = {
    val left  = t.take(idx).trim
    val right = t.drop(idx + op.length).trim
    if (left.nonEmpty && right.nonEmpty && !left.contains("(") && !right.contains("("))
      Some(Comparison(op, parseTerm(left), parseTerm(right)))
    else None
  }

  def parseBodyLiteral(text: String): BodyLiteral = {
    val t = text.trim
    val comparison = comparators.view
      .flatMap(op => findTopLevelOp(t, op).flatMap(idx => comparisonAt(t, op, idx)))
      .headOption
    comparison.getOrElse {
      if (t.startsWith("not ") || t.startsWith("not(") || t.startsWith("!")) {
        val rest = if (t.startsWith("!")) t.drop(1) else t.drop(4)
        AtomLit(parseAtom(rest, negated = true))
      } else AtomLit(parseAtom(t))
    }
  }

  def findTopLevelOp(text: String, op: String): Option[Int] = {
    @tailrec def scan(i: Int, depth: Int, inQuote: Option[Char]): Option[Int] =
      if (i >= text.length) None
      else inQuote match {
        case Some(q) => scan(i + 1, depth, if (text(i) == q) None else inQuote)
        case None =>
          val ch = text(i)
          if (ch == '\'' || ch == '"') scan(i + 1, depth, Some(ch))
          else if (ch == '(') scan(i + 1, depth + 1, None)
          else if (ch == ')') scan(i + 1, depth - 1, None)
          else if (depth == 0 && text.startsWith(op, i)) {
            val skipOp = op match {
              case "=" =>
                val prev = if (i > 0) text(i - 1) else ' '
                val next = if (i + 1 < text.length) text(i + 1) else ' '
                "<>=!".contains(prev) || next == '='
              case "<" | ">" => i + 1 < text.length && text(i + 1) == '='
              case _         => false
            }
            if (skipOp) scan(i + 1, depth, None) else Some(i)
          } else scan(i + 1, depth, None)
      }
    scan(0, 0, Option.empty[Char])
  }

  def splitTopLevel(text: String, sep: Char = ','): List[String] = {
    val (_, _, partsRev, buf) = text.foldLeft((0, Option.empty[Char], List.empty[String], List.empty[Char])) {
      case ((depth, inQuote, parts, buf), ch) => inQuote match {
        case Some(q) => (depth, if (ch == q) None else inQuote, parts, ch :: buf)
        case None =>
          if (ch == '\'' || ch == '"') (depth, Some(ch), parts, ch :: buf)
          else if (ch == '(') (depth + 1, None, parts, ch :: buf)
          else if (ch == ')') (depth - 1, None, parts, ch :: buf)
          else if (ch == sep && depth == 0) (depth, None, buf.reverse.mkString :: parts, Nil)
          else (depth, None, parts, ch :: buf)
      }
    }
    (buf.reverse.mkString :: partsRev).reverse
  }

  private def stripComments(text: String): String = {
    def commentCut(line: String): String = {
      @tailrec def scan(i: Int, inQuote: Option[Char]): Option[Int] =
        if (i >= line.length) None
        else inQuote match {
          case Some(q) => scan(i + 1, if (line(i) == q) None else inQuote)
          case None =>
            if (line(i) == '\'' || line(i) == '"') scan(i + 1, Some(line(i)))
            else if (line(i) == '#' || line(i) == '%') Some(i)
            else scan(i + 1, None)
        }
      scan(0, Option.empty[Char]).fold(line)(line.take)
    }
    text.split("\n").map(commentCut).mkString("\n")
  }

  private def splitStatements(text: String): List[String] = {
    val (_, _, stmtsRev, buf) = text.zipWithIndex.foldLeft((0, Option.empty[Char], List.empty[String], List.empty[Char])) {
      case ((depth, inQuote, stmts, buf), (ch, i)) => inQuote match {
        case Some(q) => (depth, if (ch == q) None else inQuote, stmts, ch :: buf)
        case None =>
          val prev = if (i > 0) text(i - 1) else ' '
          val next = if (i + 1 < text.length) text(i + 1) else ' '
          if (ch == '\'' || ch == '"') (depth, Some(ch), stmts, ch :: buf)
          else if (ch == '(') (depth + 1, None, stmts, ch :: buf)
          else if (ch == ')') (depth - 1, None, stmts, ch :: buf)
          else if (ch == '.' && depth == 0) {
            if (prev.isDigit && next.isDigit) (depth, None, stmts, ch :: buf)
            else (depth, None, buf.reverse.mkString :: stmts, Nil)
          } else (depth, None, stmts, ch :: buf)
      }
    }
    val last = buf.reverse.mkString
    if (last.trim.nonEmpty) (last :: stmtsRev).reverse else stmtsRev.reverse
  }
}
