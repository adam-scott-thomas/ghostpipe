"""Pipeline runner — sequential steps with parallel groups.

Steps are bare functions. Order is explicit. Data flows linearly.
Parallel groups run independent steps on the same input and merge outputs.

Usage:
    pipe = Pipeline("my_pipeline", steps=[
        Step("parse", parse_fn),
        Step("normalize", normalize_fn),
        Parallel([
            Step("score_a", score_a_fn),
            Step("score_b", score_b_fn),
        ]),
        Step("aggregate", aggregate_fn),
    ])
    result = pipe.run(raw_input)
"""
from __future__ import annotations

import hashlib
import json
import time
import concurrent.futures
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union


@dataclass(frozen=True)
class StepResult:
    """Result of a single step execution."""
    name: str
    status: str          # "success" | "failed" | "skipped"
    output: Any = None
    error: str = ""
    duration_ms: float = 0
    output_hash: str = ""


@dataclass
class PipelineResult:
    """Final result of a pipeline run."""
    pipeline_name: str
    status: str = "pending"   # "complete" | "failed" | "partial"
    steps: list[StepResult] = field(default_factory=list)
    outputs: dict[str, Any] = field(default_factory=dict)
    failed_step: Optional[str] = None
    duration_ms: float = 0

    @property
    def completed(self) -> list[str]:
        return [s.name for s in self.steps if s.status == "success"]

    @property
    def step_count(self) -> int:
        return len(self.steps)

    def get(self, step_name: str) -> Any:
        """Get output of a specific step."""
        return self.outputs.get(step_name)


class Step:
    """A single pipeline step — wraps a bare function."""

    def __init__(self, name: str, fn: Callable, *, skip_on_none: bool = False) -> None:
        self.name = name
        self.fn = fn
        self.skip_on_none = skip_on_none

    def __repr__(self) -> str:
        return f"Step({self.name!r})"


class Parallel:
    """A group of steps that run concurrently on the same input.

    Each step in the group receives the same input (output of the previous step).
    Results are merged into a dict: {step_name: step_output}.
    """

    def __init__(self, steps: list[Step], *, max_workers: int = 4) -> None:
        self.steps = steps
        self.max_workers = max_workers
        self.name = f"parallel[{','.join(s.name for s in steps)}]"

    def __repr__(self) -> str:
        return f"Parallel({[s.name for s in self.steps]})"


# Type for pipeline step entries
StepEntry = Union[Step, Parallel]


def _hash_output(output: Any) -> str:
    """SHA-256 hash of step output for audit trail."""
    try:
        canonical = json.dumps(output, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]
    except (TypeError, ValueError):
        return hashlib.sha256(repr(output).encode()).hexdigest()[:16]


def _run_step_fn(fn: Callable, input_data: Any) -> Any:
    """Call a step function with input data."""
    return fn(input_data)


class Pipeline:
    """Linear pipeline with optional parallel groups.

    Args:
        name: Pipeline identifier (for logging/audit)
        steps: Ordered list of Step and/or Parallel entries
        on_step_start: Optional callback(step_name, input_data)
        on_step_complete: Optional callback(step_name, output, duration_ms)
        on_step_error: Optional callback(step_name, error)
        audit: Optional ghostseal SealClient for audit trail
        halt_on_error: If True (default), stop pipeline on first error.
                       If False, skip failed step and continue.
    """

    def __init__(
        self,
        name: str,
        steps: list[StepEntry],
        *,
        on_step_start: Optional[Callable] = None,
        on_step_complete: Optional[Callable] = None,
        on_step_error: Optional[Callable] = None,
        audit: Any = None,
        halt_on_error: bool = True,
    ) -> None:
        self.name = name
        self.steps = steps
        self._on_start = on_step_start
        self._on_complete = on_step_complete
        self._on_error = on_step_error
        self._audit = audit
        self._halt_on_error = halt_on_error

    def run(self, input_data: Any = None) -> PipelineResult:
        """Execute the pipeline. Returns PipelineResult with all outputs."""
        pipe_start = time.time()
        result = PipelineResult(pipeline_name=self.name)
        current = input_data

        for entry in self.steps:
            if isinstance(entry, Parallel):
                step_result, current = self._run_parallel(entry, current)
                for sr in step_result:
                    result.steps.append(sr)
                    result.outputs[sr.name] = sr.output
                    if sr.status == "failed" and self._halt_on_error:
                        result.status = "failed"
                        result.failed_step = sr.name
                        result.duration_ms = (time.time() - pipe_start) * 1000
                        return result
            else:
                sr = self._run_single(entry, current)
                result.steps.append(sr)
                result.outputs[sr.name] = sr.output

                if sr.status == "failed":
                    if self._halt_on_error:
                        result.status = "failed"
                        result.failed_step = sr.name
                        result.duration_ms = (time.time() - pipe_start) * 1000
                        return result
                elif sr.status == "success":
                    current = sr.output

        result.status = "complete"
        result.duration_ms = (time.time() - pipe_start) * 1000

        self._emit_audit("pipeline.complete", {
            "pipeline": self.name,
            "steps_completed": len(result.completed),
            "duration_ms": result.duration_ms,
        })

        return result

    def _run_single(self, step: Step, input_data: Any) -> StepResult:
        """Execute a single step."""
        if step.skip_on_none and input_data is None:
            return StepResult(name=step.name, status="skipped")

        if self._on_start:
            self._on_start(step.name, input_data)

        self._emit_audit("pipeline.step.start", {"pipeline": self.name, "step": step.name})

        start = time.time()
        try:
            output = _run_step_fn(step.fn, input_data)
            duration = (time.time() - start) * 1000
            output_hash = _hash_output(output)

            if self._on_complete:
                self._on_complete(step.name, output, duration)

            self._emit_audit("pipeline.step.complete", {
                "pipeline": self.name,
                "step": step.name,
                "duration_ms": duration,
                "output_hash": output_hash,
            })

            return StepResult(
                name=step.name, status="success",
                output=output, duration_ms=duration, output_hash=output_hash,
            )
        except Exception as exc:
            duration = (time.time() - start) * 1000
            error_msg = f"{type(exc).__name__}: {exc}"

            if self._on_error:
                self._on_error(step.name, exc)

            self._emit_audit("pipeline.step.failed", {
                "pipeline": self.name,
                "step": step.name,
                "error": error_msg,
                "duration_ms": duration,
            })

            return StepResult(
                name=step.name, status="failed",
                error=error_msg, duration_ms=duration,
            )

    def _run_parallel(self, group: Parallel, input_data: Any) -> tuple[list[StepResult], dict]:
        """Execute a parallel group. All steps get the same input."""
        results: list[StepResult] = []
        merged: dict[str, Any] = {}

        self._emit_audit("pipeline.parallel.start", {
            "pipeline": self.name,
            "group": group.name,
            "steps": [s.name for s in group.steps],
        })

        with concurrent.futures.ThreadPoolExecutor(max_workers=group.max_workers) as executor:
            futures = {}
            for step in group.steps:
                if self._on_start:
                    self._on_start(step.name, input_data)
                futures[executor.submit(_run_step_fn, step.fn, input_data)] = step

            for future in concurrent.futures.as_completed(futures):
                step = futures[future]
                start_approx = time.time()
                try:
                    output = future.result()
                    duration = 0  # approximate — thread timing is imprecise
                    output_hash = _hash_output(output)
                    sr = StepResult(
                        name=step.name, status="success",
                        output=output, output_hash=output_hash,
                    )
                    merged[step.name] = output

                    if self._on_complete:
                        self._on_complete(step.name, output, duration)

                except Exception as exc:
                    error_msg = f"{type(exc).__name__}: {exc}"
                    sr = StepResult(name=step.name, status="failed", error=error_msg)
                    merged[step.name] = None

                    if self._on_error:
                        self._on_error(step.name, exc)

                results.append(sr)

        self._emit_audit("pipeline.parallel.complete", {
            "pipeline": self.name,
            "group": group.name,
            "results": {s.name: s.status for s in results},
        })

        return results, merged

    def _emit_audit(self, event_type: str, data: dict) -> None:
        """Emit to ghostseal if available."""
        if self._audit:
            try:
                self._audit.emit(event_type, data)
            except Exception:
                pass
