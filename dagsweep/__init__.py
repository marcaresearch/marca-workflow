"""dagsweep -- declarative, prefix-reusing sweeps over a step DAG.

Define steps as connectors (named inputs/outputs, optional, with a list of
variants to sweep), wire them implicitly by port names, and let the executor
topologically order them and sweep every combination -- computing each shared
prefix exactly once. Problem-agnostic: it knows nothing about your algorithms,
only ports and variants.

    from dagsweep import Step, Pipeline

    pipe = Pipeline([
        Step("rank",  run_rank,  consumes=("rules", "measures"),
             produces="ranked", variants=rankers),
        Step("prune", run_prune, consumes=("ranked",),
             produces="pruned", variants=pruners),
    ])
    results = pipe.run(seed={"rules": rules, "measures": im}, sink=evaluate)
"""

from ._step import Step
from ._pipeline import Pipeline, PipelineError
from ._executor import run

__all__ = ["Step", "Pipeline", "PipelineError", "run"]
__version__ = "0.1.0"
