# marca-workflow

Declarative, prefix-reusing workflow sweeps for MARCA. Problem-agnostic: it
knows nothing about your algorithms -- only **ports** and **variants**.

You describe each step as a connector (named inputs/outputs, optional, with a
list of variants to sweep). marca-workflow wires the steps by port name,
topologically orders them, and sweeps every combination -- computing each **shared prefix
exactly once** and reusing it for every child. The reuse is keyed on the chosen
variant indices (a cheap token), never on a hash of the data, so large
intermediate objects cost nothing to cache.

## Why

A grid sweep `A × B × C` is a prefix tree: consecutive configurations share a
long prefix. Hand-written nested loops capture that reuse but are rigid — a new
step means editing the loop. marca-workflow keeps the reuse but makes the structure
declarative: add a step by appending one `Step`.

## Example

```python
from marca_workflow import Step, Pipeline

pipe = Pipeline([
    Step("rank",  run_rank,  consumes=("rules", "measures"),
         produces="ranked", variants=rankers),
    Step("prune", run_prune, consumes=("ranked",),
         produces="pruned", variants=pruners),
    Step("clf",   run_clf,   consumes=("pruned",),
         produces="model",  variants=classifiers),
])

results = pipe.run(
    seed={"rules": rules, "measures": im},
    sink=lambda ctx: evaluate(ctx["model"]),
)
# [("BordaRank+M1Prune+OrdinalClassifier", <metric>), ...]  one record per leaf
```

`rank` runs once per ranker, `prune` once per (ranker, pruner), `clf` once per
leaf — automatically.

## The one rule: steps must be pure

`fn(variant, *inputs) -> output` must not mutate its inputs after returning
them. The executor memoizes and shares outputs across configurations; mutating a
shared output in place corrupts siblings. Purity is also what makes per-step
parallelism safe (see `Step.parallel`, reserved).

## Concepts

- **Port** — a named value in the run context. A step `produces` one port and
  `consumes` zero or more. Ports not produced by any step are *seeds*, supplied
  to `run`.
- **Variant** — one choice on a step's sweep axis. `variants=(None,)` is a step
  with no algorithm choice (a fixed transform).
- **Optional step** — `optional=True`: when the chosen variant is `None` the
  step is skipped and its output is taken from `fallback`. Put `None` in
  `variants` to sweep "with and without" the step.
- **Wiring is implicit** — `consumes`/`produces` names form the DAG; declaration
  order does not matter, dependencies decide execution order.

Validation rejects duplicate writers, cycles, unreachable fallbacks, and missing
seeds with clear errors.
