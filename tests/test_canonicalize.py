"""Tests for the public :mod:`mareforma.canonicalize` registry.

Distinct from :mod:`mareforma._canonical` (the internal envelope
canonicalizer): the public registry lets adapters register specialty
forms (SMILES, FASTA, PDB) that downstream replay can pick by name.
"""

from __future__ import annotations

import math

import pytest

from mareforma.canonicalize import (
    CanonicalizationError,
    DEFAULT_CANONICALIZER,
    canonicalize,
    canonicalize_default,
    digest_bytes,
    fingerprint_tool_config,
    register_canonicalizer,
    registered_canonicalizers,
)


def test_default_canonicalizer_registered():
    assert DEFAULT_CANONICALIZER in registered_canonicalizers()


def test_default_is_byte_stable_under_key_reorder():
    a = canonicalize({"x": 1, "y": 2})
    b = canonicalize({"y": 2, "x": 1})
    assert a == b


def test_default_rejects_nan():
    with pytest.raises(CanonicalizationError):
        canonicalize_default(float("nan"))


def test_default_rejects_inf_in_nested():
    with pytest.raises(CanonicalizationError):
        canonicalize_default({"a": [1, math.inf]})


def test_unknown_form_raises():
    with pytest.raises(CanonicalizationError) as ei:
        canonicalize({"x": 1}, form="no-such-form-v1")
    assert "no-such-form-v1" in str(ei.value)


def test_register_canonicalizer_invalid_name():
    with pytest.raises(ValueError):
        register_canonicalizer("", lambda v: b"")
    with pytest.raises(ValueError):
        register_canonicalizer("bad name with spaces", lambda v: b"")


def test_register_and_apply_custom_form():
    register_canonicalizer("upper-bytes-v1", lambda v: v.upper().encode())
    try:
        assert canonicalize("abc", form="upper-bytes-v1") == b"ABC"
    finally:
        # Module-level registry is shared — clean up to prevent leakage
        # into other tests in the same session.
        from mareforma.canonicalize import _REGISTRY
        _REGISTRY.pop("upper-bytes-v1", None)


def test_digest_bytes_shape():
    h = digest_bytes(b"abc")
    assert len(h) == 64
    assert int(h, 16) >= 0


def test_fingerprint_tool_config_shape():
    fp = fingerprint_tool_config({"model": "x", "temperature": 0.7})
    assert fp.startswith("sha256:")
    assert len(fp) == len("sha256:") + 64


def test_specialty_registration_on_import():
    """Importing the specialty module must register all three forms."""
    import mareforma.canonicalize.specialty  # noqa: F401
    names = registered_canonicalizers()
    assert "rdkit-canonical-smiles-v1" in names
    assert "fasta-nfc-v1" in names
    assert "pdb-atom-sorted-v1" in names


def test_fasta_canonicalizer_normalizes():
    from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v1
    assert canonicalize_fasta_nfc_v1("  acgtACGT  \n") == b"ACGTACGT"


def test_pdb_canonicalizer_sorts_atom_block():
    from mareforma.canonicalize.specialty import canonicalize_pdb_atom_sorted_v1
    pdb = (
        "HEADER test\n"
        "ATOM      2  CA  ALA A   1\n"
        "ATOM      1  N   ALA A   1\n"
        "END\n"
    )
    out = canonicalize_pdb_atom_sorted_v1(pdb).decode()
    lines = out.strip().split("\n")
    assert lines[0] == "HEADER test"
    assert "ATOM      1" in lines[1]
    assert "ATOM      2" in lines[2]
    assert lines[3] == "END"


def test_rdkit_canonicalizer_fallback_path():
    """In CI rdkit may be absent; fallback returns NFC-stripped bytes."""
    from mareforma.canonicalize.specialty import (
        canonicalize_rdkit_canonical_smiles_v1,
        rdkit_fallback_used,
    )
    out = canonicalize_rdkit_canonical_smiles_v1("  CCO  ")
    if rdkit_fallback_used():
        assert out == b"CCO"
    else:
        # Both rdkit and fallback should accept the same molecule.
        assert isinstance(out, bytes)
        assert len(out) > 0
