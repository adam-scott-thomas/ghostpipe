# ghostpipe

Linear pipeline runner with parallel groups. Steps are functions. Order is explicit. Zero dependencies.

## Install

```bash
pip install ghostpipe
```

## Usage

```python
from ghostpipe import Pipeline, Step, Parallel

pipe = Pipeline("assessment", steps=[
    Step("parse", parse_uploads),
    Step("normalize", normalize_data),
    Parallel([
        Step("clarity", score_clarity),
        Step("context", score_context),
        Step("iteration", score_iteration),
    ]),
    Step("aggregate", aggregate_scores),
])

result = pipe.run(raw_input)
# result.status = "complete"
# result.completed = ["parse", "normalize", "clarity", "context", "iteration", "aggregate"]
# result.get("clarity") → 0.82
# result.get("aggregate") → {"overall": 0.78}
```

## How it works

- **Step** wraps a bare function. Output of one step is input to the next.
- **Parallel** runs multiple steps on the same input concurrently. Outputs merge into a dict for the next step.
- **Errors** halt the pipeline by default (`halt_on_error=False` to continue).
- **Callbacks** for step start/complete/error.
- **Audit** via ghostseal — every step boundary emits an event with output hash.

## Parallel groups

Steps in a `Parallel` group receive the **same input** and run in threads. Results are merged into a dict:

```python
pipe = Pipeline("score", steps=[
    Step("prep", prep_fn),
    Parallel([
        Step("x2", lambda x: x * 2),
        Step("x3", lambda x: x * 3),
    ]),
    Step("sum", lambda d: d["x2"] + d["x3"]),
])

result = pipe.run(10)
# result.get("sum") = 50
```

Order within the group doesn't matter. Same result every time.

## With ghostseal audit

```python
from ghostseal import SealClient

audit = SealClient(blackbox_url="https://blackbox:8443", api_key="...")
pipe = Pipeline("assessment", steps=[...], audit=audit)
pipe.run(data)
# Every step start/complete/fail emits to Blackbox
```

## Part of the GhostLogic SDK

```
maelspine   → config registry
ghostseal   → audit backbone
ghostprompt → prompt management
ghostpipe   → pipeline runner (this package)
ghostrouter → LLM routing
ghostserver → MCP tools
```

## License

Apache 2.0
