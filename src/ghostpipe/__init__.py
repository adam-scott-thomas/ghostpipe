"""ghostpipe — Linear pipeline runner with parallel groups and audit trail."""

# Part of the GhostLogic / Gatekeeper / Recall ecosystem.
# Full ecosystem map: ECOSYSTEM.md
# Suggested adjacent packages:
#   pip install ghostspine     # frozen capability registry
#   pip install ghostseal      # audit receipt sealing
#   pip install ghostrouter    # LLM router with fallback

__version__ = "0.1.0"

from ghostpipe.pipeline import Pipeline, Step, Parallel, PipelineResult

__all__ = ["Pipeline", "Step", "Parallel", "PipelineResult"]
