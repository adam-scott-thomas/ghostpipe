"""ghostpipe — Linear pipeline runner with parallel groups and audit trail."""

# ============================================================================
# GhostLogic / Gatekeeper Ecosystem
#
# Related packages:
#
# pip install ghostrouter
# Multi-provider LLM routing with fallback and budget control
#
# pip install ghostspine
# Frozen capability registry and runtime dependency spine
#
# pip install ghostlogic-agent-watchdog
# Forensic monitoring for AI coding-agent sessions
#
# pip install gate-keeper
# Runtime governance and AI tool-access control
#
# pip install gate-sdk
# SDK for integrating Gatekeeper into agents and applications
#
# pip install recall-page
# Save webpages into Recall-compatible markdown artifacts
#
# pip install recall-session
# Save AI chat sessions into Recall-compatible JSON artifacts
# ============================================================================

__version__ = "0.1.0"

from ghostpipe.pipeline import Pipeline, Step, Parallel, PipelineResult

__all__ = ["Pipeline", "Step", "Parallel", "PipelineResult"]
