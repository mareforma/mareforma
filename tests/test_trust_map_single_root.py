"""The finding independence axis discloses single-root operator-Sybil topology.

The README promises the map marks independence honest "when every signer traces
back to one operator who could have made all the keys." The non-finding branch
enforces that (``n_roots < 2`` → UNVERIFIABLE), but the finding branch returned a
confident number without ever consulting ``n_roots``. A single operator can mint
several keys and submit distinct-signer findings; the effective number must not
read as cross-operator independence with no disclosure of the single-root
topology. The model-distinct number still stands (distinct models are real
evidence), but the residual names the single trust root so it is not mistaken for
independence across operators.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.trust_map import _assemble
from tests._helpers import (
    _bootstrap_key, _claim, _enroll_key, _est, _pred, _prop, _verdict,
)

_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"


class TestFindingSingleRootResidual:
    def test_single_root_finding_names_the_residual(self) -> None:
        """A finding independence number under a single trust root carries the
        single-root caveat in its residual (the finding branch consulted
        n_roots)."""
        tmap = _assemble(
            _claim(), n_roots=1, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        ind = tmap.get("independence")
        # The model-distinct number still stands: distinct models are evidence.
        assert ind.value == "2"
        # ...but the residual now discloses the single-root topology.
        assert "single trust root" in ind.residual

    def test_zero_root_finding_names_no_root_enrolled(self) -> None:
        tmap = _assemble(
            _claim(), n_roots=0, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        ind = tmap.get("independence")
        assert "no trust root is enrolled" in ind.residual

    def test_multi_root_finding_has_no_single_root_caveat(self) -> None:
        """With two or more enrolled roots the single-root caveat is absent."""
        tmap = _assemble(
            _claim(), n_roots=2, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        ind = tmap.get("independence")
        assert ind.value == "2"
        assert "single trust root" not in ind.residual

    def test_integration_single_operator_two_models(self, tmp_path: Path) -> None:
        """End to end: one operator (one auto-enrolled root) mints keys and
        submits two distinct-model findings. The map reports the number but names
        the single-root residual, so it cannot read as cross-operator
        independence."""
        ka = _bootstrap_key(tmp_path, "ka.key")  # the single auto-enrolled root
        kb = _bootstrap_key(tmp_path, "kb.key")
        # One operator mints kb and enrolls it under its own root: still a single
        # trust root (n_roots=1), but kb's distinct model authenticates on read.
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            ind = g.trust_map(r["claim_id"]).get("independence")
        assert ind.value == "2"
        assert "single trust root" in ind.residual
