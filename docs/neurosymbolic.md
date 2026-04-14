# Neurosymbolic AI — Datalog Reasoning in Loki Engine

Loki Engine ships a self-contained Datalog engine (`loki.engine.symbolic`) and a dedicated `ReasonerNode` that wires it into the graph executor. Use it whenever you need deterministic, auditable inference — classification rules, policy enforcement, knowledge-graph queries, constraint checking — without burning tokens or introducing non-determinism.

## When to use a ReasonerNode

| Use case | Example |
|----------|---------|
| Entity classification | `dangerous(X) :- chemical(X), !approved(X).` |
| Reachability in a KG | `reachable(X, Z) :- hop(X, Y), reachable(Y, Z).` |
| Policy enforcement | `allowed(Op, User) :- role(User, admin), not banned(User).` |
| Data pre-processing | Derive structured facts before an LLM agent reads them |
| Post-processing / validation | Verify LLM output satisfies logical constraints |

## Quick start

```scala
import loki.engine.core.nodes.*
import loki.engine.core.executor.*

val reasoner = ReasonerNode(
  nodeId    = "r1",
  name      = "AncestorFinder",
  program   = """
    |ancestor(X, Z) :- parent(X, Y), parent(Y, Z).
    |ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
  """.stripMargin,
  factSource = FactSource(stateKeys = List("family_facts")),
  query      = List("ancestor"),   // only emit "ancestor" facts
  outputKey  = "ancestors"         // write result to state.data("ancestors")
)

val ctx = ExecutionContext(
  query = "find ancestors",
  state = WorkflowState(data = Map(
    "family_facts" -> List(
      List("parent", "alice", "bob"),
      List("parent", "bob",   "carol"),
      List("parent", "carol", "dave")
    )
  ))
)

for {
  _      <- store.upsertNode(reasoner)
  result <- executor.run("r1", "find ancestors", executionContext = Some(ctx))
} yield result.state.data("ancestors")
// → Map(
//     "facts"       -> [{"predicate":"ancestor","args":["alice","carol"]}, ...],
//     "fact_count"  -> 6,
//     "input_count" -> 3,
//     "predicates"  -> ["ancestor"]
//   )
```

## Datalog language reference

### Variables

Both conventions are supported:

```datalog
ancestor(X, Z) :- parent(X, Y), parent(Y, Z).   // uppercase-first (Prolog style)
ancestor(?x, ?z) :- parent(?x, ?y), parent(?y, ?z).   // ?-prefix style
```

Use `_` for anonymous/wildcard variables:

```datalog
child(X) :- parent(_, X).
```

### Constants

```datalog
status(alice, active).          // bareword atom
status(bob, "active status").   // quoted string
threshold(X) :- score(X, N), N > 100.   // numeric literal
flag(X) :- enabled(X, true).    // boolean
```

### Rules

```
head(Args) :- body_lit1, body_lit2, ...
```

A rule without a body is a ground fact:

```datalog
parent(alice, bob).
```

### Negation-as-failure

Prefix a literal with `!` or `not ` for negation-as-failure:

```datalog
safe(X) :- node(X), !compromised(X).
safe(X) :- node(X), not compromised(X).  // equivalent
```

Negation is stratified: you cannot write recursion through negation (raises `StratificationError`).

### Comparison operators

`=` `==` `!=` `<` `<=` `>` `>=`

Both operands must be bound before the comparison is evaluated (safety rule):

```datalog
big(X) :- size(X, N), N > 10.
same_size(X, Y) :- size(X, N), size(Y, N).
```

### Comments

```datalog
# This is a comment
% This is also a comment
ancestor(X, Z) :- parent(X, Z).  # inline comment
```

## Fact sources

`FactSource` controls where the reasoner collects its input facts:

```scala
FactSource(
  stateKeys        = List("my_facts"),   // read from state.data keys
  edgeTypes        = List("NEXT", "DELEGATES_TO"),  // KG edges → (edgetype, src, dst) facts
  includeNodeFacts = true,               // also emit (nodeType, nodeName) for every KG node
  useNodeNames     = true                // use node.name; false = use nodeId
)
```

**Priority**: if `stateKeys` is non-empty, only those keys are read. Otherwise the reasoner looks at `state.data("facts")` and the last output in `ctx.outputs`. KG facts from `edgeTypes` / `includeNodeFacts` are always appended on top.

### KG-driven reasoning example

```scala
val kgReasoner = ReasonerNode(
  name      = "TrustAnalyser",
  program   = """
    |trusted(X) :- agent(X), !flagged(X).
    |reachable(X, Z) :- DELEGATES_TO(X, Y), reachable(Y, Z).
    |reachable(X, X) :- agent(X).
  """.stripMargin,
  factSource = FactSource(
    edgeTypes        = List("DELEGATES_TO"),
    includeNodeFacts = true,
    useNodeNames     = true
  ),
  query     = List("trusted", "reachable"),
  outputKey = "trust_analysis"
)
```

## Proof traces

Set `withProof = true` to include justification trees in the output:

```scala
val reasoner = ReasonerNode(
  ...,
  withProof = true
)
```

Output gains a `"proofs"` key:

```json
{
  "facts": [{"predicate": "ancestor", "args": ["alice", "dave"]}],
  "proofs": [
    {
      "fact": {"predicate": "ancestor", "args": ["alice", "dave"]},
      "explanation": "ancestor[\"alice\", \"dave\"] ⟸ {\"predicate\":\"parent\",\"args\":[\"alice\",\"bob\"]}, ... [rule: ancestor(X,Z):-...]"
    }
  ]
}
```

## Mixing with LLM agents

A common pattern: Reasoner validates or pre-processes facts, then an Agent consumes the structured result.

```
[InputContext]──▶[ReasonerNode]──▶[AgentNode (analyst)]──▶ __END__
```

```scala
val graph = GraphNode(
  entryNodeId = "r1",
  exitNodeId  = "agent1",
  strategy    = "sequential",
  inputKeys   = List("raw_data"),     // pull raw_data into the subgraph
  scopeOutputs = false                // share outputs with parent so agent sees reasoner output
)
```

The agent can then read from `state.data("inferred")` (the reasoner's `outputKey`) in its system prompt context.

## Using the Datalog engine directly

The engine is available as a pure Scala library with no ZIO dependency:

```scala
import loki.engine.symbolic.*

// Parse a program
val rules = parseProgram("""
  |ancestor(X, Z) :- parent(X, Y), parent(Y, Z).
  |ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
""".stripMargin)

// Provide input facts
val inputFacts = List(
  fact("parent", "alice", "bob"),
  fact("parent", "bob",   "carol")
)

// Solve
val solution = new Program(rules).solve(inputFacts, withProof = true)

// Query
solution.query("ancestor").foreach(f => println(factToDict(f)))
// → {predicate -> ancestor, args -> [alice, carol]}
// → {predicate -> ancestor, args -> [bob, carol]}

// Explain a derivation
val f = solution.derived.head
println(solution.explain(f))
```

### Rule objects (Map-style)

You can also pass rules as Map objects — useful when rules come from a database or API:

```scala
val ruleObj: Any = Map(
  "head" -> Map("predicate" -> "connected", "terms" -> List("A", "B")),
  "body" -> List(
    Map("predicate" -> "link", "terms" -> List("A", "B"))
  )
)
val rules = ruleFromObj(ruleObj)
```

## Safety rules

The engine enforces safety automatically:

- **Unsafe head variable**: every variable in the rule head must appear in at least one positive body literal → throws `UnsafeRuleError`
- **Unsafe negation/comparison**: variables in negated atoms or comparisons must already be bound by positive literals → throws `UnsafeRuleError`
- **Recursive negation**: negation through a recursive predicate cycle → throws `StratificationError`

## Checkpoint integration

`ReasonerNode` outputs land in `state.data[outputKey]` and are included in checkpoints automatically:

```scala
// Save after reasoning step
val cp = executor.checkpointContext(ctx, "post-reasoning")

// Later — inspect and resume
val readiness = executor.inspectResume(ctx, requiredOutputs = List("inferred"))
if (readiness.ok) executor.resumeFromCheckpoint(cp.nodeId, "agent1")
```

See [long-running-workflows.md](long-running-workflows.md) for the full checkpoint/resume guide.
