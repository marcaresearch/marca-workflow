from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class Step:
    """One node of a sweep DAG, wired by named input/output ports.

    A step is a connector: it ``consumes`` named ports (produced by upstream
    steps or supplied in the run seed) and ``produces`` one named port. It may
    carry a list of ``variants`` (the algorithms/objects to sweep); the executor
    keys its memo on the chosen variant index, so a step is computed once per
    distinct upstream-choice prefix and reused by every child.

    Contract: ``fn`` must be **pure** with respect to its output -- it must not
    mutate the objects it received after returning them. The executor memoizes
    and shares outputs across configurations; in-place mutation of a shared
    output corrupts siblings. (Purity is also what makes per-step parallelism
    safe.)

    Attributes:
        name:     unique step id, also the default output port name.
        fn:       ``fn(variant, *consumed_values) -> output``. For steps with no
                  algorithm choice use ``variants=(None,)`` and ignore the first arg.
        consumes: input port names, in the order ``fn`` expects them.
        produces: output port name (defaults to ``name``).
        variants: objects to sweep; the choice key is the index into this tuple.
        optional: when ``True`` and the chosen variant is ``None``, the step is
                  skipped and its output is taken from ``fallback``.
        fallback: port name whose value becomes this step's output when skipped.
        parallel: hint that this step's variants are safe/worth running in
                  parallel. Reserved -- the reference executor runs sequentially.
    """

    name: str
    fn: Callable[..., Any]
    consumes: tuple[str, ...] = ()
    produces: str = ""
    variants: tuple[Any, ...] = (None,)
    optional: bool = False
    fallback: str | None = None
    parallel: bool = False

    def __post_init__(self):
        # Normalise list args to tuples and default produces -> name.
        object.__setattr__(self, "consumes", tuple(self.consumes))
        object.__setattr__(self, "variants", tuple(self.variants))
        if not self.produces:
            object.__setattr__(self, "produces", self.name)
        if self.optional and self.fallback is None:
            # An optional step with no fallback yields None when skipped.
            pass
