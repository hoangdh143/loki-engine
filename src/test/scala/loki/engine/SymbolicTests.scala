package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import loki.engine.symbolic._

object SymbolicTests {

  def spec = suite("SymbolicTests")(

    test("fact() constructs ground facts") {
      val f = fact("parent", "alice", "bob")
      assert(f)(Assertion.equalTo(List("parent", "alice", "bob")))
    },

    test("factToDict encodes predicate and args") {
      val f  = fact("likes", "alice", "chess")
      val d  = factToDict(f)
      assert(d("predicate"))(Assertion.equalTo("likes")) &&
      assert(d("args"))(Assertion.equalTo(List("alice", "chess")))
    },

    test("normaliseFact lifts a plain list") {
      val in = List("parent", "alice", "bob"): Any
      assert(normaliseFact(in))(Assertion.equalTo(List("parent", "alice", "bob")))
    },

    test("normaliseFact lifts a Map with predicate key") {
      val in: Any = Map("predicate" -> "foo", "args" -> List("x", "y"))
      val out = normaliseFact(in)
      assert(out.head)(Assertion.equalTo("foo")) &&
      assert(out.tail)(Assertion.equalTo(List("x", "y")))
    },

    test("empty program returns only input facts") {
      val program = new Program(Nil)
      val input   = Set(fact("a", "x"), fact("b", "y"))
      val sol     = program.solve(input.toList)
      assert(sol.derived)(Assertion.isEmpty) &&
      assert(sol.facts)(Assertion.equalTo(input))
    },

    test("simple transitivity rule derives new facts") {
      val rules = parseProgram("""
        |ancestor(X, Z) :- parent(X, Y), parent(Y, Z).
      """.stripMargin)
      val program = new Program(rules)
      val input = List(
        fact("parent", "alice", "bob"),
        fact("parent", "bob",   "carol")
      )
      val sol = program.solve(input)
      assert(sol.derived)(Assertion.contains(List("ancestor", "alice", "carol")))
    },

    test("recursive ancestor rule derives transitive closure") {
      val rules = parseProgram("""
        |ancestor(X, Y) :- parent(X, Y).
        |ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
      """.stripMargin)
      val program = new Program(rules)
      val input = List(
        fact("parent", "a", "b"),
        fact("parent", "b", "c"),
        fact("parent", "c", "d")
      )
      val sol = program.solve(input)
      assert(sol.derived)(Assertion.contains(List("ancestor", "a", "d")))
    },

    test("negation-as-failure excludes matched facts") {
      val rules = parseProgram("""
        |safe(X) :- node(X), !infected(X).
      """.stripMargin)
      val program = new Program(rules)
      val input = List(
        fact("node",     "a"),
        fact("node",     "b"),
        fact("infected", "a")
      )
      val sol = program.solve(input)
      assert(sol.derived)(Assertion.contains(List("safe", "b"))) &&
      assert(sol.derived)(Assertion.not(Assertion.contains(List("safe", "a"))))
    },

    test("emitDerivedOnly=false includes input facts in solution.facts") {
      val rules = parseProgram("reachable(X) :- node(X).")
      val p     = new Program(rules)
      val input = List(fact("node", "x"))
      val sol   = p.solve(input)
      assert(sol.facts)(Assertion.contains(fact("node", "x"))) &&
      assert(sol.facts)(Assertion.contains(fact("reachable", "x")))
    },

    test("withProof records a justification entry") {
      val rules = parseProgram("big(X) :- size(X, large).")
      val p     = new Program(rules)
      val input = List(fact("size", "elephant", "large"))
      val sol   = p.solve(input, withProof = true)
      val derived = sol.derived.find(_.headOption.contains("big"))
      assert(derived.isDefined)(Assertion.isTrue) &&
      assert(sol.justifications.contains(derived.get))(Assertion.isTrue)
    },

    test("parseProgram handles multi-rule programs") {
      val src = """
        |parent(X, Y) :- father(X, Y).
        |parent(X, Y) :- mother(X, Y).
        |ancestor(X, Y) :- parent(X, Y).
      """.stripMargin
      val rules = parseProgram(src)
      assert(rules.length)(Assertion.equalTo(3))
    },

    test("ruleFromObj parses a Map-style rule object") {
      val obj: Any = Map(
        "head" -> Map("predicate" -> "connected", "terms" -> List("A", "B")),
        "body" -> List(Map("predicate" -> "link", "terms" -> List("A", "B")))
      )
      val rules = ruleFromObj(obj)
      assert(rules.length)(Assertion.equalTo(1)) &&
      assert(rules.head.head.predicate)(Assertion.equalTo("connected"))
    },

    test("comparison operator filters via >") {
      val rules = parseProgram("big(X) :- size(X, N), N > 10.")
      val p     = new Program(rules)
      val input = List(
        fact("size", "a", 15),
        fact("size", "b", 5)
      )
      val sol = p.solve(input)
      assert(sol.derived)(Assertion.contains(List("big", "a"))) &&
      assert(sol.derived)(Assertion.not(Assertion.contains(List("big", "b"))))
    },

    test("Solution.query filters by predicate") {
      val rules = parseProgram("child(X) :- parent(_, X).")
      val p     = new Program(rules)
      val input = List(fact("parent", "alice", "bob"), fact("parent", "alice", "carol"))
      val sol   = p.solve(input)
      val kids  = sol.query("child")
      assert(kids.length)(Assertion.equalTo(2))
    }
  )
}
