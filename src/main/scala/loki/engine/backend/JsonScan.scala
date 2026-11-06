package loki.engine.backend

import scala.annotation.tailrec

/** Balanced-brace JSON scanning shared by the OpenAI and Anthropic backends.
  *
  * The upstream tool-call extraction predates the project's JSON parser: it
  * fishes `type: function` / `type: tool_use` objects out of a response body
  * with index walks, so a local model's mangled output can still be recovered.
  * These scans are tail-recursive index walks — no local mutation. Braces
  * inside string values are not quote-aware, matching the original scanners. */
object JsonScan {

  @tailrec def skipWs(s: String, i: Int): Int =
    if (i < s.length && s(i).isWhitespace) skipWs(s, i + 1) else i

  /** Index of the balanced closing brace for the object opening at `open`,
    * or -1 when the object never closes. */
  @tailrec def balancedEnd(s: String, open: Int, depth: Int = 0): Int =
    if (open >= s.length) -1
    else s(open) match {
      case '{' => balancedEnd(s, open + 1, depth + 1)
      case '}' => if (depth - 1 == 0) open else balancedEnd(s, open + 1, depth - 1)
      case _   => balancedEnd(s, open + 1, depth)
    }

  /** Walk backwards from `idx` to the opening brace of the enclosing object.
    * Mirrors the original `while (start > 0 && s(start) != '{')` walk: an index
    * that reaches 0 without finding a brace returns 0. */
  @tailrec def openBraceBefore(s: String, idx: Int): Int =
    if (idx <= 0) idx.max(0)
    else if (s(idx) == '{') idx
    else openBraceBefore(s, idx - 1)

  private def indexOrMax(s: String, needle: String, from: Int): Int = {
    val i = s.indexOf(needle, from)
    if (i < 0) Int.MaxValue else i
  }

  /** Every balanced {...} object containing either spelling of `needle`, in
    * document order. Unlike the original `.max(indexOf, indexOf)` pair — which
    * silently skipped an earlier call when a body mixed both spellings — the
    * closest occurrence wins. */
  def findBlocks(body: String, needleA: String, needleB: String): List[String] = {
    @tailrec def next(from: Int, acc: List[String]): List[String] =
      if (from >= body.length) acc.reverse
      else {
        val idx = math.min(indexOrMax(body, needleA, from), indexOrMax(body, needleB, from))
        if (idx >= body.length) acc.reverse
        else {
          val start = openBraceBefore(body, idx)
          val end = balancedEnd(body, start)
          if (end < 0) acc.reverse
          else next(end + 1, body.substring(start, end + 1) :: acc)
        }
      }
    next(0, Nil)
  }

  /** The decoded value of `"key": "..."` in a flat object, or None when the
    * key, colon, opening quote, or closing quote is missing. Handles the
    * standard JSON escapes, including a \uXXXX whose hex is unparseable
    * (falls back to a literal 'u', as before). */
  def extractStr(json: String, key: String): Option[String] = {
    val needle = "\"" + key + "\""
    val keyStart = json.indexOf(needle)
    if (keyStart < 0) None
    else {
      val afterKey = skipWs(json, keyStart + needle.length)
      if (afterKey >= json.length || json(afterKey) != ':') None
      else {
        val afterColon = skipWs(json, afterKey + 1)
        if (afterColon >= json.length || json(afterColon) != '"') None
        else decodeString(json, afterColon + 1, escaped = false, acc = Nil)
      }
    }
  }

  private val stringKey = """"(\w+)"\s*:\s*"""".r

  /** Index of the quote closing the string whose content starts at `i`, or -1. */
  @tailrec private def stringEnd(s: String, i: Int): Int =
    if (i >= s.length) -1
    else if (s(i) == '\\') stringEnd(s, i + 2)
    else if (s(i) == '"') i
    else stringEnd(s, i + 1)

  /** Every `"key": "value"` pair in the text, with the values decoded. Values are walked by
    * index because a regex over them recurses per character: a long tool argument overflows
    * the stack, which is fatal to the whole JVM. */
  def stringFields(json: String): List[(String, String)] = {
    val keys = stringKey.pattern.matcher(json)

    @tailrec def loop(from: Int, acc: List[(String, String)]): List[(String, String)] =
      if (!keys.find(from)) acc.reverse
      else {
        val key = keys.group(1)
        val start = keys.end()
        val end = stringEnd(json, start)
        if (end < 0) acc.reverse
        else loop(end + 1, decodeString(json, start, false, Nil).fold(acc)(value => (key, value) :: acc))
      }

    loop(0, Nil)
  }

  @tailrec private def decodeString(json: String, i: Int, escaped: Boolean, acc: List[Char]): Option[String] =
    if (i >= json.length) None
    else if (escaped) {
      json(i) match {
        case '"'  => decodeString(json, i + 1, false, '"' :: acc)
        case '\\' => decodeString(json, i + 1, false, '\\' :: acc)
        case '/'  => decodeString(json, i + 1, false, '/' :: acc)
        case 'b'  => decodeString(json, i + 1, false, '\b' :: acc)
        case 'f'  => decodeString(json, i + 1, false, '\f' :: acc)
        case 'n'  => decodeString(json, i + 1, false, '\n' :: acc)
        case 'r'  => decodeString(json, i + 1, false, '\r' :: acc)
        case 't'  => decodeString(json, i + 1, false, '\t' :: acc)
        case 'u' if i + 4 < json.length =>
          val hex = try Some(Integer.parseInt(json.substring(i + 1, i + 5), 16).toChar)
                    catch { case _: NumberFormatException => None }
          hex match {
            case Some(c) => decodeString(json, i + 5, false, c :: acc)
            case None    => decodeString(json, i + 1, false, 'u' :: acc)
          }
        case other => decodeString(json, i + 1, false, other :: acc)
      }
    }
    else if (json(i) == '\\') decodeString(json, i + 1, true, acc)
    else if (json(i) == '"') Some(acc.reverse.mkString)
    else decodeString(json, i + 1, false, json(i) :: acc)
}
