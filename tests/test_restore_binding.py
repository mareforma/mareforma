"""Restore's signed-field binding, and the remedy restore names.

Restore re-checks every signed predicate field against the row it rebuilds,
so a backup whose TOML was edited under a still-valid envelope is caught.
Two things about that check are load-bearing here.

First, the signature covers canonical bytes, and canonicalization
NFC-normalizes every string, while the row keeps the bytes the caller
passed. Text that arrives decomposed is ordinary: macOS filenames, PDF
extracts, most text typed in Korean or Vietnamese. Comparing the two with
a bare ``!=`` reports a mismatch on input nobody touched. Restore is
all-or-nothing, so one such claim costs the operator every other claim in
the project, and the refusal names the operator as the tamperer.

Second, restore warns when a recovered claim carries no signature in a
project that enrols a validator. That warning is the only place restore
tells an operator what to do about a loss that does not reverse, so any
command it names has to exist and the way out it describes has to be real.
"""

from __future__ import annotations

import importlib
import re
import unicodedata
import warnings
from pathlib import Path

import pytest
import tomli_w

import mareforma
from mareforma.db import RestoreError
from tests._helpers import _bootstrap_key

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover -- 3.10 path
    import tomli as tomllib  # type: ignore[no-redef]


# "Cá" written as C + a + U+0301 COMBINING ACUTE ACCENT. Canonicalization
# composes it to U+00E1 before signing; the row keeps what was passed.
_DECOMPOSED = "Cá uptake rose in the treated cohort"
_COMPOSED = unicodedata.normalize("NFC", _DECOMPOSED)


def _sanity() -> None:
    assert _COMPOSED != _DECOMPOSED, "fixture no longer exercises NFC divergence"


def _wipe_graph_db(root: Path) -> None:
    db_dir = root / ".mareforma"
    for f in db_dir.iterdir():
        f.unlink()
    db_dir.rmdir()


def _signed_project(root: Path, text: str) -> tuple[Path, str, str]:
    """Build a signing project holding an ASCII anchor plus one claim of *text*.

    The anchor exists to price the refusal: restore is all-or-nothing, so a
    claim the check wrongly rejects takes the anchor down with it.
    """
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        anchor = g.assert_claim(
            "plain ascii anchor", generated_by="seed", seed=True,
        )
        subject = g.assert_claim(text, generated_by="agent-a")
    return key, anchor, subject


def _read_toml(root: Path) -> dict:
    return tomllib.loads((root / "claims.toml").read_text(encoding="utf-8"))


def _write_toml(root: Path, data: dict) -> None:
    (root / "claims.toml").write_bytes(tomli_w.dumps(data).encode("utf-8"))


# ---------------------------------------------------------------------------
# NFC form on the signed-field binding
# ---------------------------------------------------------------------------

def test_restore_accepts_a_claim_whose_text_arrived_decomposed(
    tmp_path: Path,
) -> None:
    """Decomposed text is signed composed and stored decomposed. Restore has
    to read that as the same value, because the signature already does."""
    _sanity()
    key, anchor, subject = _signed_project(tmp_path, _DECOMPOSED)
    assert _DECOMPOSED in (tmp_path / "claims.toml").read_text(encoding="utf-8")

    _wipe_graph_db(tmp_path)
    result = mareforma.restore(tmp_path)

    assert result["claims_restored"] == 2
    with mareforma.open(tmp_path, key_path=key) as g:
        # Byte-for-byte what the caller wrote: restore rebuilds the row, it
        # does not rewrite the operator's text into another form.
        assert g.get_claim(subject)["text"] == _DECOMPOSED
        # The price of getting this wrong, pinned: every other claim.
        assert g.get_claim(anchor) is not None


def test_restore_accepts_a_decomposed_source_name(tmp_path: Path) -> None:
    """``source_name`` is signed on the same tuple as ``text`` and takes the
    same divergence, so the fix has to cover the whole loop, not one field."""
    _sanity()
    key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=key) as g:
        subject = g.assert_claim(
            "an ascii finding",
            generated_by="agent-a",
            source_name=_DECOMPOSED,
        )

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path)

    with mareforma.open(tmp_path, key_path=key) as g:
        assert g.get_claim(subject)["source_name"] == _DECOMPOSED


def test_restore_still_refuses_text_edited_after_signing(
    tmp_path: Path,
) -> None:
    """The gate narrows to NFC form and nothing else. A value that still
    differs after normalization is still a refusal."""
    key, anchor, subject = _signed_project(tmp_path, "an honest finding")
    data = _read_toml(tmp_path)
    data["claims"][subject]["text"] = "a finding nobody signed"
    _write_toml(tmp_path, data)

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as caught:
        mareforma.restore(tmp_path)

    assert caught.value.kind == "claim_unverified"
    assert "signed-predicate field 'text'" in str(caught.value)


def test_restore_refuses_text_swapped_to_a_different_composition_target(
    tmp_path: Path,
) -> None:
    """Not every combining sequence is benign. Text edited to a value that
    normalizes to something else is caught, so the relaxation cannot be
    walked into a laundering path for a hand-edited row."""
    _sanity()
    key, anchor, subject = _signed_project(tmp_path, _DECOMPOSED)
    data = _read_toml(tmp_path)
    # Same base letter, different accent: composes to "à", not "á".
    data["claims"][subject]["text"] = _DECOMPOSED.replace("́", "̀")
    _write_toml(tmp_path, data)

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as caught:
        mareforma.restore(tmp_path)

    assert caught.value.kind == "claim_unverified"


# ---------------------------------------------------------------------------
# The remedy restore names for an unsigned claim
# ---------------------------------------------------------------------------

def _restore_with_one_unsigned_claim(root: Path) -> str:
    """Recover a signing project holding one keyless claim; return the warning.

    Dropping the bundle alone reads as a de-signed claim and is refused, so
    the fixture drops the signing residue with it: this is the shape of a
    claim written by a run that never held the key.
    """
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        g.assert_claim("anchor", generated_by="seed", seed=True)
        loose = g.assert_claim("written without the key", generated_by="agent-a")

    data = _read_toml(root)
    for column in ("signature_bundle", "statement_cid", "asserter_keyid"):
        data["claims"][loose].pop(column, None)
    _write_toml(root, data)

    _wipe_graph_db(root)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = mareforma.restore(root)
    assert result["unsigned_in_signed_mode"] == 1

    messages = [
        str(w.message) for w in caught
        if str(w.message).startswith("restore: ")
    ]
    assert len(messages) == 1, messages
    return messages[0]


def _cli_resolves(parts: list[str]) -> bool:
    """True when ``mareforma <parts>`` names a command the shipped CLI has."""
    from mareforma.cli import cli

    node = cli
    for part in parts:
        commands = getattr(node, "commands", None)
        if not commands or part not in commands:
            return False
        node = commands[part]
    return True


def test_the_shipped_cli_has_no_cover_pre_signing_command() -> None:
    """The retroactive-signature repair was built and withdrawn before
    release. Pinned here so the guard below cannot be read as pedantry."""
    assert not _cli_resolves(["validator", "cover-pre-signing"])
    # The two that did ship, so the resolver is proven to say yes as well.
    assert _cli_resolves(["validator", "add"])
    assert _cli_resolves(["validator", "list"])


def test_restore_only_names_commands_the_cli_actually_has() -> None:
    """Every backticked ``mareforma …`` command in the restore module has to
    resolve. A recovery path that invents a command sends an operator who
    just lost a graph chasing one that never shipped."""
    # ``mareforma.db.restore`` is the re-exported function; reach the module.
    module = importlib.import_module("mareforma.db.restore")
    source = Path(module.__file__).read_text(encoding="utf-8")
    named = re.findall(r"`mareforma ([^`]+)`", source)
    unresolved = [
        text for text in named
        if not _cli_resolves(
            [p for p in text.split() if not p.startswith("-")][:2],
        )
    ]
    assert not unresolved, (
        f"restore.py names commands the CLI does not have: {unresolved}"
    )


def test_unsigned_claim_warning_states_the_real_remedy(tmp_path: Path) -> None:
    """The disclosed loss is permanent and the operator gets one route out:
    assert the findings again under the project's key."""
    message = _restore_with_one_unsigned_claim(tmp_path)

    assert "cover-pre-signing" not in message
    # What is true: enrolment does not reverse, and nothing signs after the
    # fact, so the warning has to say both rather than point at a command.
    assert "un-enrol" in message
    assert "again" in message and "key" in message
    # The disclosure itself is unchanged: still counted, still named.
    assert message.startswith("restore: 1 claim(s) carry no signature")
