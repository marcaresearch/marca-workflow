from __future__ import annotations

from itertools import product
from typing import Any, Callable

from ._pipeline import Pipeline, PipelineError


def _default_name(choices: list[tuple[Any, Any]]) -> str:
    """Join the chosen non-None variants by ``.name`` (or class name)."""
    parts = []
    for _step, variant in choices:
        if variant is None:
            continue
        parts.append(getattr(variant, "name", None) or type(variant).__name__)
    return "+".join(parts) if parts else "pipeline"


def run(
    pipeline: Pipeline,
    seed: dict[str, Any],
    sink: Callable[[dict[str, Any]], Any],
    name_fn: Callable[[list[tuple[Any, Any]]], str] | None = None,
) -> dict[str, Any]:
    """Sweep every variant combination, computing each prefix once.

    For each full combination of variant choices the steps run in topological
    order, building a context of port values. Each step's output is memoized on
    ``(step, chosen-variants-of-its-ancestors)`` -- a cheap token tuple, never a
    hash of the data -- so shared prefixes are computed once and reused. The
    terminal context is handed to ``sink`` to produce that combination's result.

    Returns ``[(name_fn(choices), sink(context)), ...]`` -- one record per leaf,
    in sweep order. A list (not a dict) so configurations that resolve to the
    same name are all kept.
    """
    name_fn = name_fn or _default_name
    order = pipeline.order
    ancestors = pipeline.ancestors

    missing_seed = _missing_seeds(pipeline, seed)
    if missing_seed:
        raise PipelineError(f"Missing seed port(s): {sorted(missing_seed)}")

    results: list[tuple[str, Any]] = []
    memo: dict[tuple, Any] = {}
    ranges = [range(len(s.variants)) for s in order]

    for combo in product(*ranges):
        ctx = dict(seed)
        for i, step in enumerate(order):
            key = (i, tuple(combo[j] for j in ancestors[i]))
            if key in memo:
                out = memo[key]
            else:
                variant = step.variants[combo[i]]
                if step.optional and variant is None:
                    out = ctx[step.fallback] if step.fallback is not None else None
                else:
                    out = step.fn(variant, *(ctx[c] for c in step.consumes))
                memo[key] = out
            ctx[step.produces] = out
        choices = [(step, step.variants[combo[i]]) for i, step in enumerate(order)]
        results.append((name_fn(choices), sink(ctx)))
    return results


def _missing_seeds(pipeline: Pipeline, seed: dict[str, Any]) -> set[str]:
    produced = {s.produces for s in pipeline.order}
    needed: set[str] = set()
    for s in pipeline.order:
        for port in s.consumes:
            if port not in produced and port not in seed:
                needed.add(port)
        if s.optional and s.fallback is not None:
            if s.fallback not in produced and s.fallback not in seed:
                needed.add(s.fallback)
    return needed
