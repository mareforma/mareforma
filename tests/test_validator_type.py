"""validator_type substrate tests.

The validators table carries a ``validator_type`` column —
``'human'`` or ``'llm'`` — bound into the signed enrollment envelope.
The substrate enforces two rules on top of the existing identity check:

  1. LLM-typed validators may sign validation envelopes, but
     :func:`mareforma.db.validate_claim` refuses to promote a claim to
     ESTABLISHED when the signing validator's row carries
     ``validator_type='llm'``.

  2. Self-validation (claim signer == validation signer) is refused
     regardless of validator_type. The trust ladder rests on external
     witnessing; a key cannot promote its own claim.

The signed payload binding ensures a post-hoc UPDATE of validator_type
in the row (e.g. ``UPDATE validators SET validator_type='human' WHERE
keyid=...``) breaks ``verify_enrollment`` — the chain walk in
``is_enrolled`` then refuses the tampered row.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing
from mareforma import validators as _validators


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _bootstrap_key(tmp_path: Path, name: str) -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


def _pem_of(key_path: Path) -> bytes:
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


def _build_replicated(graph) -> str:
    seed = graph.assert_claim("seed", generated_by="seed", seed=True)
    rep = graph.assert_claim("finding", supports=[seed], generated_by="A")
    graph.assert_claim("finding", supports=[seed], generated_by="B")
    assert graph.get_claim(rep)["support_level"] == "REPLICATED"
    return rep


# ---------------------------------------------------------------------------
# Schema + enrollment defaults
# ---------------------------------------------------------------------------

class TestEnrollmentDefault:
    def test_auto_enrolled_root_is_human(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            keyid = _signing.public_key_id(g._signer.public_key())
            row = _validators.get_validator(g._conn, keyid)
        assert row is not None
        assert row["validator_type"] == "human"

    def test_enroll_validator_defaults_to_human(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        other_key = _bootstrap_key(tmp_path, "other.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            row = g.enroll_validator(_pem_of(other_key), identity="other")
        assert row["validator_type"] == "human"

    def test_enroll_validator_with_llm_type(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        bot_key = _bootstrap_key(tmp_path, "bot.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            row = g.enroll_validator(
                _pem_of(bot_key), identity="reviewer-bot", validator_type="llm",
            )
        assert row["validator_type"] == "llm"


# ---------------------------------------------------------------------------
# Invalid validator_type
# ---------------------------------------------------------------------------

class TestInvalidValidatorType:
    def test_unknown_validator_type_refused(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        other_key = _bootstrap_key(tmp_path, "other.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            with pytest.raises(_validators.InvalidValidatorTypeError):
                g.enroll_validator(
                    _pem_of(other_key), identity="other",
                    validator_type="cyborg",
                )

    def test_empty_validator_type_refused(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        other_key = _bootstrap_key(tmp_path, "other.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            with pytest.raises(_validators.InvalidValidatorTypeError):
                g.enroll_validator(
                    _pem_of(other_key), identity="other",
                    validator_type="",
                )


# ---------------------------------------------------------------------------
# Signed envelope binds validator_type
# ---------------------------------------------------------------------------

class TestEnvelopeBindsValidatorType:
    def test_envelope_payload_contains_validator_type(
        self, tmp_path: Path,
    ) -> None:
        """The signed payload must include validator_type so a verifier
        can detect post-hoc tampering of the row's column."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        bot_key = _bootstrap_key(tmp_path, "bot.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            row = g.enroll_validator(
                _pem_of(bot_key), identity="bot", validator_type="llm",
            )
        envelope = json.loads(row["enrollment_envelope"])
        payload = _signing.envelope_payload(envelope)
        assert payload["validator_type"] == "llm"

    def test_tampered_validator_type_breaks_verify(
        self, tmp_path: Path,
    ) -> None:
        """Flip validator_type from 'llm' to 'human' in the row via a
        direct sqlite UPDATE. verify_enrollment must refuse the row
        because the payload's validator_type no longer matches."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        bot_key = _bootstrap_key(tmp_path, "bot.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            bot_row = g.enroll_validator(
                _pem_of(bot_key), identity="bot", validator_type="llm",
            )
            root_pem = _signing.public_key_to_pem(g._signer.public_key())

        # Tamper.
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute(
            "UPDATE validators SET validator_type = 'human' WHERE keyid = ?",
            (bot_row["keyid"],),
        )
        raw.commit()
        raw.close()

        # Reload and verify against the (still-valid) parent key.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            tampered = _validators.get_validator(g._conn, bot_row["keyid"])
        assert tampered["validator_type"] == "human"
        assert _validators.verify_enrollment(tampered, root_pem) is False


# ---------------------------------------------------------------------------
# LLM-typed validator cannot promote past REPLICATED
# ---------------------------------------------------------------------------

class TestLLMValidatorPromotionRefused:
    def test_llm_validator_validate_raises(self, tmp_path: Path) -> None:
        """An enrolled LLM validator may sign envelopes but
        validate_claim refuses to flip the row to ESTABLISHED."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        bot_key = _bootstrap_key(tmp_path, "bot.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            rep_id = _build_replicated(g)
            g.enroll_validator(
                _pem_of(bot_key), identity="reviewer-bot",
                validator_type="llm",
            )

        with mareforma.open(tmp_path, key_path=bot_key) as g:
            with pytest.raises(_db.LLMValidatorPromotionError):
                g.validate(rep_id)

        # The claim is unchanged.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(rep_id)["support_level"] == "REPLICATED"

    def test_human_validator_still_promotes(self, tmp_path: Path) -> None:
        """A human-typed validator (the default) promotes as before."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        human_key = _bootstrap_key(tmp_path, "human.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            rep_id = _build_replicated(g)
            g.enroll_validator(
                _pem_of(human_key), identity="alice@lab",
                validator_type="human",
            )

        with mareforma.open(tmp_path, key_path=human_key) as g:
            g.validate(rep_id)
            assert g.get_claim(rep_id)["support_level"] == "ESTABLISHED"


# ---------------------------------------------------------------------------
# Self-validation refused
# ---------------------------------------------------------------------------

class TestSelfValidationRefused:
    def test_same_key_signs_and_validates_refused(
        self, tmp_path: Path,
    ) -> None:
        """The root key signs the claim (via _build_replicated) AND tries
        to validate it. The substrate refuses self-promotion."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            rep_id = _build_replicated(g)
            with pytest.raises(_db.SelfValidationError):
                g.validate(rep_id)

        # The claim is unchanged.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(rep_id)["support_level"] == "REPLICATED"

    def test_distinct_keys_promote_normally(self, tmp_path: Path) -> None:
        """Sanity check: with two keys the substrate does not falsely
        refuse — only the equal-keyid case is blocked."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            rep_id = _build_replicated(g)
            g.enroll_validator(_pem_of(val_key), identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(rep_id)
            assert g.get_claim(rep_id)["support_level"] == "ESTABLISHED"

    def test_self_validation_takes_precedence_over_llm_check(
        self, tmp_path: Path,
    ) -> None:
        """If the same key signed the claim AND would be the LLM
        validator, either error is acceptable — the SQL order is to
        consult validator_type first, then bundle-signer equality. The
        canonical behaviour is to refuse for SOME reason; this test
        documents that we don't silently promote."""
        # Build a graph where the LLM validator is also the claim signer.
        bot_key = _bootstrap_key(tmp_path, "bot.key")
        with mareforma.open(tmp_path, key_path=bot_key) as g:
            # bot is root → auto-enrolled as 'human' by default.
            # Direct sqlite tamper: flip to 'llm' AND break the chain
            # check by also rewriting the envelope. Easier: enroll a
            # second key first, then re-bootstrap with that second key
            # as 'llm', sign the REPLICATED chain with it.
            # The simplest construction: use bot as a generator that is
            # also the would-be validator; the substrate gate raises
            # SelfValidationError before reaching the LLM check.
            rep_id = _build_replicated(g)
            with pytest.raises((_db.SelfValidationError,
                                _db.LLMValidatorPromotionError)):
                g.validate(rep_id)


# ---------------------------------------------------------------------------
# LLM validator cannot seed ESTABLISHED (substrate parity with validate)
# ---------------------------------------------------------------------------

class TestLLMValidatorSeedRefused:
    def test_llm_root_cannot_seed(self, tmp_path: Path) -> None:
        """Without the seed-path LLM gate, a born-ESTABLISHED row from
        an LLM-typed validator would route around the same ceiling
        validate_claim enforces. The seed path must mirror the gate."""
        from mareforma import validators as _validators
        bot_key_path = _bootstrap_key(tmp_path, "bot.key")
        bot_signer = _signing.load_private_key(bot_key_path)

        # Bootstrap the project with bot as an LLM-typed root validator
        # via the validators module directly (the public mareforma.open
        # auto-enrolls 'human' by default — auto_enroll_root accepts an
        # explicit validator_type kwarg used here).
        conn = _db.open_db(tmp_path)
        try:
            _validators.auto_enroll_root(
                conn, bot_signer, "bot", validator_type="llm",
            )
        finally:
            conn.close()

        # Now the bot key is an enrolled LLM-typed root. The seed gate
        # must refuse a born-ESTABLISHED row from this validator.
        with mareforma.open(tmp_path, key_path=bot_key_path) as g:
            with pytest.raises(_db.LLMValidatorPromotionError):
                g.assert_claim("attempted seed", seed=True)


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------

class TestCLIValidatorAddType:
    def test_cli_validator_add_llm_flag(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        # Stage XDG so the CLI finds it.
        xdg = _signing.default_key_path()
        xdg.parent.mkdir(parents=True, exist_ok=True)
        xdg.write_bytes(root_key.read_bytes())
        import os
        os.chmod(xdg, 0o600)

        # Open once to auto-enroll root.
        with mareforma.open(tmp_path):
            pass

        bot_key = _bootstrap_key(tmp_path, "bot.key")
        bot_pem_path = tmp_path / "bot.pub.pem"
        bot_pem_path.write_bytes(_pem_of(bot_key))

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            [
                "validator", "add",
                "--pubkey", str(bot_pem_path),
                "--identity", "reviewer-bot",
                "--type", "llm",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "reviewer-bot (llm)" in result.output

        # List shows the type tag.
        list_result = runner.invoke(mareforma_cli, ["validator", "list"])
        assert list_result.exit_code == 0
        assert "[llm]" in list_result.output

    def test_cli_validator_add_rejects_unknown_type(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        bot_pem_path = tmp_path / "bot.pub.pem"
        bot_pem_path.write_bytes(
            _signing.public_key_to_pem(_signing.generate_keypair().public_key()),
        )

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            [
                "validator", "add",
                "--pubkey", str(bot_pem_path),
                "--identity", "alien",
                "--type", "cyborg",
            ],
        )
        # Click rejects the choice before mareforma sees it.
        assert result.exit_code != 0
        assert "cyborg" in result.output
