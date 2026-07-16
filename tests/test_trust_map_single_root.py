"""The finding independence axis discloses single-root operator-Sybil topology.

Under a single trust root the operator owns every enrolled key, so every axis of
distinctness is operator-assertable: the signer keys are mintable and the model
lineage each finding binds is signed by the operator's own key, so a distinct
model is not cross-checked by an independent party. The independence number is a
count, not certified cross-operator independence; the residual names the
single-root topology on both the signer and the model axis so the number is not
over-read. A certified number needs distinct trust roots.
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
        single-root caveat in its residual."""
        tmap = _assemble(
            _claim(), n_roots=1, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        ind = tmap.get("independence")
        # The number is a count; the residual discloses the single-root topology.
        assert ind.value == "2"
        assert "single trust root" in ind.residual

    def test_single_root_residual_names_the_model_axis(self) -> None:
        """Under a single trust root the residual discloses that the model axis,
        not only the signer, is operator-assertable, so the count is not read as
        certified cross-model independence. The operator owns the enrolled key
        that signs each finding's lineage and can re-sign a fabricated distinct
        model, so the distinctness is producer-assertable."""
        tmap = _assemble(
            _claim(), n_roots=1, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        residual = tmap.get("independence").residual
        assert "model lineage is signed by the operator's own key" in residual
        assert "producer-assertable" in residual
        assert "not certified independence across operators" in residual

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
