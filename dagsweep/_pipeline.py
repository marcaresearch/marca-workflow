from __future__ import annotations

from typing import Iterable

from ._step import Step


class PipelineError(ValueError):
    """Raised when the declared steps do not form a valid sweep DAG."""


class Pipeline:
    """A validated, topologically ordered DAG of :class:`Step` connectors.

    The wiring is implicit: a step that ``consumes`` a port is connected to the
    step that ``produces`` it. Construction validates single-writer ports, the
    absence of cycles, and resolvable fallbacks, then computes a topological
    order and, for each step, the set of steps it transitively depends on (used
    by the executor to key its prefix-reuse memo).

    Names not produced by any step are treated as *seed* ports, supplied at
    :func:`dagsweep.run` time.
    """

    def __init__(self, steps: Iterable[Step]):
        self.steps: list[Step] = list(steps)
        if not self.steps:
            raise PipelineError("Pipeline needs at least one step.")
        self._validate_unique_produces()
        self.order: list[Step] = self._topo_sort()
        self._pos: dict[str, int] = {s.produces: i for i, s in enumerate(self.order)}
        self.ancestors: list[tuple[int, ...]] = self._compute_ancestors()
        self._validate_fallbacks()

    # ------------------------------------------------------------------
    # validation / structure
    # ------------------------------------------------------------------

    def _validate_unique_produces(self):
        seen: dict[str, str] = {}
        for s in self.steps:
            if s.produces in seen:
                raise PipelineError(
                    f"Port {s.produces!r} is produced by both {seen[s.produces]!r} "
                    f"and {s.name!r}; each port needs a single writer."
                )
            seen[s.produces] = s.name

    def _producers(self) -> dict[str, Step]:
        return {s.produces: s for s in self.steps}

    def _topo_sort(self) -> list[Step]:
        producers = self._producers()
        # edges: producer -> consumer for every consumed port that some step produces
        indeg: dict[str, int] = {s.name: 0 for s in self.steps}
        deps: dict[str, list[str]] = {s.name: [] for s in self.steps}  # name -> dependents
        by_name = {s.name: s for s in self.steps}
        for s in self.steps:
            for port in s.consumes:
                producer = producers.get(port)
                if producer is not None and producer.name != s.name:
                    deps[producer.name].append(s.name)
                    indeg[s.name] += 1
        # Kahn, stable on declaration order for reproducibility
        ready = [s.name for s in self.steps if indeg[s.name] == 0]
        order: list[Step] = []
        while ready:
            name = ready.pop(0)
            order.append(by_name[name])
            for dependent in deps[name]:
                indeg[dependent] -= 1
                if indeg[dependent] == 0:
                    ready.append(dependent)
        if len(order) != len(self.steps):
            stuck = [n for n, d in indeg.items() if d > 0]
            raise PipelineError(f"Cycle detected among steps: {sorted(stuck)}")
        return order

    def _compute_ancestors(self) -> list[tuple[int, ...]]:
        """For each step (by position in ``order``) the sorted set of positions it
        transitively depends on, including itself. Keys the executor's memo."""
        anc: list[set[int]] = []
        for i, s in enumerate(self.order):
            acc: set[int] = {i}
            for port in s.consumes:
                p = self._pos.get(port)
                if p is not None:  # produced upstream (else it's a seed)
                    acc |= anc[p]
            anc.append(acc)
        return [tuple(sorted(a)) for a in anc]

    def _validate_fallbacks(self):
        produced = set(self._pos)
        for i, s in enumerate(self.order):
            if s.optional and s.fallback is not None:
                upstream = {self.order[j].produces for j in self.ancestors[i] if j != i}
                # fallback must be a seed (not produced here) or an upstream port
                if s.fallback in produced and s.fallback not in upstream:
                    raise PipelineError(
                        f"Step {s.name!r} fallback {s.fallback!r} is produced "
                        f"downstream of it, not available when skipped."
                    )

    # ------------------------------------------------------------------
    def run(self, seed, sink, name_fn=None):
        from ._executor import run as _run
        return _run(self, seed, sink, name_fn=name_fn)

    def iter_run(self, seed, sink, name_fn=None):
        from ._executor import iter_run as _iter_run
        return _iter_run(self, seed, sink, name_fn=name_fn)
