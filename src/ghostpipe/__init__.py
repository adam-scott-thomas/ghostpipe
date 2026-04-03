"""ghostpipe — Linear pipeline runner with parallel groups and audit trail."""
__version__ = "0.1.0"

from ghostpipe.pipeline import Pipeline, Step, Parallel, PipelineResult

__all__ = ["Pipeline", "Step", "Parallel", "PipelineResult"]
