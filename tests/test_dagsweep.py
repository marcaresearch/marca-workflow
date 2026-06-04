"""dagsweep engine tests -- problem-agnostic, stdlib only."""

import pytest

from dagsweep import Step, Pipeline, PipelineError


class Counter:
    """A swept 'algorithm' that records how many times it ran."""

    def __init__(self, name):
        self.name = name
        self.calls = 0

    def __call__(self, value):
        self.calls += 1
        return f"{value}->{self.name}"


def _chain():
    """A -> B -> C linear chain, 2 variants each."""
    A = [Counter("a0"), Counter("a1")]
    B = [Counter("b0"), Counter("b1")]
    C = [Counter("c0"), Counter("c1")]
    pipe = Pipeline([
        Step("A", lambda v, seed: v(seed), consumes=("seed",), produces="a", variants=A),
        Step("B", lambda v, a: v(a), consumes=("a",), produces="b", variants=B),
        Step("C", lambda v, b: v(b), consumes=("b",), produces="c", variants=C),
    ])
    return pipe, A, B, C


def test_prefix_reuse_call_counts():
    """Each step runs once per distinct upstream-choice prefix, not per leaf."""
    pipe, A, B, C = _chain()
    results = pipe.run(seed={"seed": "x"}, sink=lambda ctx: ctx["c"])
    assert len(results) == 8  # 2*2*2 leaves
    # A: 2 prefixes, B: 4, C: 8. Naive (no reuse) would be 8/8/8.
    assert sum(a.calls for a in A) == 2
    assert sum(b.calls for b in B) == 4
    assert sum(c.calls for c in C) == 8


def test_values_match_bruteforce():
    pipe, A, B, C = _chain()
    results = pipe.run(seed={"seed": "x"}, sink=lambda ctx: ctx["c"])
    expected = {
        f"{a.name}+{b.name}+{c.name}": f"x->{a.name}->{b.name}->{c.name}"
        for a in [type("o", (), {"name": n})() for n in ("a0", "a1")]
        for b in [type("o", (), {"name": n})() for n in ("b0", "b1")]
        for c in [type("o", (), {"name": n})() for n in ("c0", "c1")]
    }
    assert results == expected


def test_optional_skip_uses_fallback():
    ran = {"n": 0}

    def sel(v, a):
        ran["n"] += 1
        return f"sel({a})"

    sel_algo = type("S", (), {"name": "S"})()
    pipe = Pipeline([
        Step("A", lambda v: "A", produces="a", variants=[None]),
        Step("S", sel, consumes=("a",), produces="s",
             variants=[None, sel_algo], optional=True, fallback="a"),
        Step("Z", lambda v, s: f"Z({s})", consumes=("s",), produces="z", variants=[None]),
    ])
    results = pipe.run(seed={}, sink=lambda ctx: ctx["z"])
    # one leaf skips S (fallback to 'a'='A'), one runs S
    assert set(results.values()) == {"Z(A)", "Z(sel(A))"}
    assert ran["n"] == 1  # S ran only on the non-None variant


def test_diamond_branching():
    """B and C consume A; D consumes both -> ancestor union, A reused."""
    A = [Counter("a0"), Counter("a1")]
    pipe = Pipeline([
        Step("A", lambda v, s: v(s), consumes=("s",), produces="a", variants=A),
        Step("B", lambda v, a: f"B({a})", consumes=("a",), produces="b"),
        Step("C", lambda v, a: f"C({a})", consumes=("a",), produces="c"),
        Step("D", lambda v, b, c: f"{b}|{c}", consumes=("b", "c"), produces="d"),
    ])
    results = pipe.run(seed={"s": "x"}, sink=lambda ctx: ctx["d"])
    assert len(results) == 2  # only A is swept
    assert sum(a.calls for a in A) == 2  # A computed once per variant, shared by B and C
    assert results["a0"] == "B(x->a0)|C(x->a0)"


def test_topological_order_independent_of_declaration():
    """Steps declared out of dependency order still run correctly."""
    pipe = Pipeline([
        Step("C", lambda v, b: b + "c", consumes=("b",), produces="c"),
        Step("A", lambda v, s: s + "a", consumes=("s",), produces="a"),
        Step("B", lambda v, a: a + "b", consumes=("a",), produces="b"),
    ])
    out = pipe.run(seed={"s": "x"}, sink=lambda ctx: ctx["c"])
    assert list(out.values()) == ["xabc"]


def test_duplicate_produces_rejected():
    with pytest.raises(PipelineError, match="single writer"):
        Pipeline([
            Step("A", lambda v: 1, produces="x"),
            Step("B", lambda v: 2, produces="x"),
        ])


def test_cycle_rejected():
    with pytest.raises(PipelineError, match="Cycle"):
        Pipeline([
            Step("A", lambda v, b: b, consumes=("b",), produces="a"),
            Step("B", lambda v, a: a, consumes=("a",), produces="b"),
        ])


def test_missing_seed_rejected():
    pipe = Pipeline([Step("A", lambda v, s: s, consumes=("s",), produces="a")])
    with pytest.raises(PipelineError, match="Missing seed"):
        pipe.run(seed={}, sink=lambda ctx: ctx["a"])
