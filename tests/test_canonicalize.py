"""Tests for the public :mod:`mareforma.canonicalize` registry.

Distinct from :mod:`mareforma._canonical` (the internal envelope
canonicalizer): the public registry lets adapters register specialty
forms (SMILES, FASTA, PDB) that downstream replay can pick by name.

Conceptual clusters:

- :class:`TestDefaultCanonicalizer` — ``json-c14n-v1`` behaviour and
  non-finite rejection.
- :class:`TestRegistry` — register / lookup / errors / cleanup.
- :class:`TestUtilities` — ``digest_bytes`` + ``fingerprint_tool_config``.
- :class:`TestSpecialtyForms` — RDKit / FASTA / PDB canonicalizers.
- :class:`TestSpecialtyAutoImport` — docstring contract: importing the
  parent package registers the specialty forms.
- :class:`TestDsseJcsNfcV1` — the DSSE envelope canonicaliser
  exposed under a registered name, NFC normalising.
"""

from __future__ import annotations

import math

import pytest

from mareforma.canonicalize import (
    CanonicalizationError,
    DEFAULT_CANONICALIZER,
    DSSE_JCS_NFC_V1,
    canonicalize,
    canonicalize_default,
    digest_bytes,
    fingerprint_tool_config,
    register_canonicalizer,
    registered_canonicalizers,
)


class TestDefaultCanonicalizer:
    def test_default_canonicalizer_registered(self):
        assert DEFAULT_CANONICALIZER in registered_canonicalizers()

    def test_byte_stable_under_key_reorder(self):
        a = canonicalize({"x": 1, "y": 2})
        b = canonicalize({"y": 2, "x": 1})
        assert a == b

    def test_rejects_nan(self):
        with pytest.raises(CanonicalizationError):
            canonicalize_default(float("nan"))

    def test_rejects_inf_in_nested(self):
        with pytest.raises(CanonicalizationError):
            canonicalize_default({"a": [1, math.inf]})


class TestRegistry:
    def test_unknown_form_raises(self):
        with pytest.raises(CanonicalizationError) as ei:
            canonicalize({"x": 1}, form="no-such-form-v1")
        assert "no-such-form-v1" in str(ei.value)

    def test_register_invalid_name(self):
        with pytest.raises(ValueError):
            register_canonicalizer("", lambda v: b"")
        with pytest.raises(ValueError):
            register_canonicalizer("bad name with spaces", lambda v: b"")

    def test_register_and_apply_custom_form(self):
        register_canonicalizer("upper-bytes-v1", lambda v: v.upper().encode())
        try:
            assert canonicalize("abc", form="upper-bytes-v1") == b"ABC"
        finally:
            # Module-level registry is shared — clean up to prevent
            # leakage into other tests in the same session.
            from mareforma.canonicalize import _REGISTRY
            _REGISTRY.pop("upper-bytes-v1", None)


class TestUtilities:
    def test_digest_bytes_shape(self):
        h = digest_bytes(b"abc")
        assert len(h) == 64
        assert int(h, 16) >= 0

    def test_fingerprint_tool_config_shape(self):
        fp = fingerprint_tool_config({"model": "x", "temperature": 0.7})
        assert fp.startswith("sha256:")
        assert len(fp) == len("sha256:") + 64


class TestSpecialtyForms:
    def test_specialty_registration_on_import(self):
        """Importing the specialty module must register all three forms."""
        import mareforma.canonicalize.specialty  # noqa: F401
        names = registered_canonicalizers()
        assert "rdkit-canonical-smiles-v1" in names
        assert "fasta-nfc-v1" in names
        assert "pdb-atom-sorted-v1" in names

    def test_fasta_canonicalizer_normalizes(self):
        from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v1
        assert canonicalize_fasta_nfc_v1("  acgtACGT  \n") == b"ACGTACGT"

    def test_pdb_canonicalizer_sorts_atom_block(self):
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

    def test_rdkit_canonicalizer_fallback_path(self):
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


class TestSpecialtyAutoImport:
    def test_auto_imported_on_package_import(self):
        """Importing mareforma.canonicalize alone registers the specialty forms.

        Verifies the static import statement in __init__.py drags the
        specialty submodule in. The reload caveat (re-running __init__
        creates a new _REGISTRY but specialty.py keeps its old reference)
        makes a runtime-reload test unreliable; instead we read the source
        and assert the import line is present.
        """
        import inspect
        import mareforma.canonicalize as can
        src = inspect.getsource(can)
        assert "from mareforma.canonicalize import specialty" in src, (
            "mareforma/canonicalize/__init__.py must auto-import the "
            "specialty submodule so its forms are registered without the "
            "caller having to discover the import"
        )
        # And in the live import, all three forms are registered.
        names = set(can.registered_canonicalizers())
        assert "rdkit-canonical-smiles-v1" in names
        assert "fasta-nfc-v1" in names
        assert "pdb-atom-sorted-v1" in names


class TestDsseJcsNfcV1:
    def test_registered(self):
        """The DSSE envelope canonicaliser is exposed under a registered form."""
        assert DSSE_JCS_NFC_V1 in registered_canonicalizers()
        # And it produces the same bytes as the private envelope canonicaliser.
        from mareforma._canonical import canonicalize as envelope
        payload = {"x": 1, "name": "café"}
        assert canonicalize(payload, form=DSSE_JCS_NFC_V1) == envelope(payload)

    def test_nfc_normalises(self):
        """dsse-jcs-nfc-v1 collapses NFC vs NFD into the same bytes."""
        # 'é' as precomposed U+00E9 vs decomposed e+U+0301.
        a = canonicalize({"k": "café"}, form=DSSE_JCS_NFC_V1)
        b = canonicalize({"k": "café"}, form=DSSE_JCS_NFC_V1)
        assert a == b
