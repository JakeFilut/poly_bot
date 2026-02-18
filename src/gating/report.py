"""Gate and diagnostic report builders.

Converts :class:`GateResult` objects and raw diagnostics dicts into
structured report dicts suitable for JSONL logging.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from src.gating.gates import GateResult


class GateReporter:
    """Builds GATE_REPORT and DIAG_REPORT structures."""

    @staticmethod
    def gate_report(
        gate_result: GateResult, ctx: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Return a flat dict summarising the gate evaluation.

        Parameters
        ----------
        gate_result : GateResult
            The result returned by :meth:`GateEvaluator.evaluate`.
        ctx : dict, optional
            Extra context to merge into the report (unused for now).
        """
        return {
            "allowed": gate_result.allowed,
            "blocked": gate_result.reasons_blocked,
            "throttled": gate_result.reasons_throttled,
            "size_mult": gate_result.size_mult,
            "edge_bump_cents": gate_result.edge_bump_cents,
            "diagnostics": gate_result.diagnostics,
        }

    @staticmethod
    def diag_report(diagnostics: Dict[str, Any]) -> Dict[str, Any]:
        """Wrap raw diagnostics in a typed DIAG_REPORT envelope."""
        return {"type": "DIAG_REPORT", **diagnostics}
