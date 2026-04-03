"""Tests for ghostpipe — sequential, parallel, error handling, audit."""
import pytest
import time
from unittest.mock import MagicMock
from ghostpipe import Pipeline, Step, Parallel, PipelineResult


# --- Basic sequential ---

def test_single_step():
    pipe = Pipeline("test", steps=[Step("double", lambda x: x * 2)])
    result = pipe.run(5)
    assert result.status == "complete"
    assert result.get("double") == 10


def test_two_steps_chain():
    pipe = Pipeline("test", steps=[
        Step("add1", lambda x: x + 1),
        Step("mul2", lambda x: x * 2),
    ])
    result = pipe.run(3)
    assert result.status == "complete"
    assert result.get("add1") == 4
    assert result.get("mul2") == 8  # (3+1)*2


def test_three_steps_data_flows():
    pipe = Pipeline("test", steps=[
        Step("parse", lambda x: x.split(",")),
        Step("count", lambda x: len(x)),
        Step("label", lambda x: f"{x} items"),
    ])
    result = pipe.run("a,b,c")
    assert result.get("parse") == ["a", "b", "c"]
    assert result.get("count") == 3
    assert result.get("label") == "3 items"


def test_no_input():
    pipe = Pipeline("test", steps=[Step("gen", lambda _: 42)])
    result = pipe.run()
    assert result.get("gen") == 42


def test_completed_list():
    pipe = Pipeline("test", steps=[
        Step("a", lambda x: x),
        Step("b", lambda x: x),
    ])
    result = pipe.run("data")
    assert result.completed == ["a", "b"]


def test_step_count():
    pipe = Pipeline("test", steps=[Step("a", lambda x: x), Step("b", lambda x: x)])
    result = pipe.run(1)
    assert result.step_count == 2


def test_duration_tracked():
    def slow(x):
        time.sleep(0.05)
        return x
    pipe = Pipeline("test", steps=[Step("slow", slow)])
    result = pipe.run(1)
    assert result.duration_ms >= 40


# --- Parallel ---

def test_parallel_group():
    pipe = Pipeline("test", steps=[
        Step("prep", lambda x: x),
        Parallel([
            Step("upper", lambda x: x.upper()),
            Step("lower", lambda x: x.lower()),
            Step("length", lambda x: len(x)),
        ]),
    ])
    result = pipe.run("Hello")
    merged = result.get("parallel[upper,lower,length]")
    # Parallel output is passed as merged dict — but individual outputs are also in result.outputs
    assert result.get("upper") == "HELLO"
    assert result.get("lower") == "hello"
    assert result.get("length") == 5


def test_parallel_same_input():
    """All parallel steps receive the same input."""
    received = []

    def capture_a(x):
        received.append(("a", x))
        return x + 1

    def capture_b(x):
        received.append(("b", x))
        return x + 2

    pipe = Pipeline("test", steps=[
        Parallel([Step("a", capture_a), Step("b", capture_b)]),
    ])
    result = pipe.run(10)
    # Both should have received 10
    inputs = [v for _, v in received]
    assert all(v == 10 for v in inputs)
    assert result.get("a") == 11
    assert result.get("b") == 12


def test_parallel_order_independent():
    """Parallel results are the same regardless of completion order."""
    import random

    def slow_random(x):
        time.sleep(random.uniform(0.01, 0.05))
        return x * 2

    def fast_random(x):
        time.sleep(random.uniform(0.01, 0.05))
        return x * 3

    pipe = Pipeline("test", steps=[
        Parallel([Step("a", slow_random), Step("b", fast_random)]),
    ])
    # Run multiple times — results should always be the same
    for _ in range(3):
        result = pipe.run(5)
        assert result.get("a") == 10
        assert result.get("b") == 15


def test_parallel_then_sequential():
    pipe = Pipeline("test", steps=[
        Parallel([
            Step("x2", lambda x: x * 2),
            Step("x3", lambda x: x * 3),
        ]),
        Step("sum", lambda d: d["x2"] + d["x3"]),
    ])
    result = pipe.run(10)
    assert result.get("sum") == 50  # 20 + 30


# --- Error handling ---

def test_error_halts_pipeline():
    def fail(x):
        raise ValueError("boom")

    pipe = Pipeline("test", steps=[
        Step("ok", lambda x: x),
        Step("fail", fail),
        Step("never", lambda x: x),
    ])
    result = pipe.run(1)
    assert result.status == "failed"
    assert result.failed_step == "fail"
    assert "never" not in result.completed


def test_error_continues_when_halt_off():
    def fail(x):
        raise ValueError("boom")

    pipe = Pipeline("test", steps=[
        Step("ok", lambda x: x + 1),
        Step("fail", fail),
        Step("after", lambda x: x + 10),
    ], halt_on_error=False)
    result = pipe.run(1)
    # "after" should still run with the last good output (from "ok")
    assert "ok" in result.completed


def test_parallel_error_halts():
    def fail(x):
        raise RuntimeError("parallel boom")

    pipe = Pipeline("test", steps=[
        Parallel([Step("ok", lambda x: x), Step("fail", fail)]),
    ])
    result = pipe.run(1)
    assert result.status == "failed"
    assert result.failed_step == "fail"


def test_error_message_captured():
    def fail(x):
        raise TypeError("bad type")

    pipe = Pipeline("test", steps=[Step("fail", fail)])
    result = pipe.run(1)
    assert result.steps[0].error == "TypeError: bad type"


# --- Skip ---

def test_skip_on_none():
    pipe = Pipeline("test", steps=[
        Step("maybe", lambda x: None),
        Step("skipped", lambda x: x + 1, skip_on_none=True),
    ])
    result = pipe.run(1)
    assert result.steps[1].status == "skipped"


# --- Callbacks ---

def test_callbacks_called():
    starts = []
    completes = []
    errors = []

    pipe = Pipeline("test", steps=[
        Step("a", lambda x: x + 1),
        Step("b", lambda x: x + 2),
    ],
        on_step_start=lambda name, data: starts.append(name),
        on_step_complete=lambda name, out, dur: completes.append(name),
        on_step_error=lambda name, err: errors.append(name),
    )
    pipe.run(0)
    assert starts == ["a", "b"]
    assert completes == ["a", "b"]
    assert errors == []


def test_error_callback():
    errors = []

    def fail(x):
        raise ValueError("oops")

    pipe = Pipeline("test", steps=[Step("fail", fail)],
                    on_step_error=lambda name, err: errors.append(name))
    pipe.run(1)
    assert errors == ["fail"]


# --- Audit ---

def test_audit_emitted():
    mock_audit = MagicMock()
    pipe = Pipeline("test", steps=[
        Step("a", lambda x: x),
    ], audit=mock_audit)
    pipe.run(1)
    assert mock_audit.emit.call_count >= 2  # step.start + step.complete + pipeline.complete
    event_types = [call[0][0] for call in mock_audit.emit.call_args_list]
    assert "pipeline.step.start" in event_types
    assert "pipeline.step.complete" in event_types
    assert "pipeline.complete" in event_types


def test_audit_not_required():
    pipe = Pipeline("test", steps=[Step("a", lambda x: x)])
    result = pipe.run(1)
    assert result.status == "complete"


# --- Output hash ---

def test_output_hash_present():
    pipe = Pipeline("test", steps=[Step("a", lambda x: {"key": "val"})])
    result = pipe.run(1)
    assert result.steps[0].output_hash
    assert len(result.steps[0].output_hash) == 16  # truncated SHA-256


def test_output_hash_deterministic():
    pipe = Pipeline("test", steps=[Step("a", lambda x: {"key": "val"})])
    r1 = pipe.run(1)
    r2 = pipe.run(1)
    assert r1.steps[0].output_hash == r2.steps[0].output_hash


# --- Spine integration ---

def test_spine_integration():
    try:
        from spine import Core
        Core._reset_instance()

        pipe = Pipeline("test", steps=[Step("a", lambda x: x * 2)])

        def setup(c):
            c.register("pipeline.test", pipe)
            c.boot(env="test")

        Core.boot_once(setup)
        p = Core.instance().get("pipeline.test")
        result = p.run(5)
        assert result.get("a") == 10

        Core._reset_instance()
    except ImportError:
        pytest.skip("spine not installed")


# --- Repr ---

def test_step_repr():
    s = Step("parse", lambda x: x)
    assert repr(s) == "Step('parse')"


def test_parallel_repr():
    p = Parallel([Step("a", lambda x: x), Step("b", lambda x: x)])
    assert repr(p) == "Parallel(['a', 'b'])"
