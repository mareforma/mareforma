"""Specialty canonicalizers: domain-specific byte-stable forms.

Importing this module registers ``rdkit-canonical-smiles-v1``,
``fasta-nfc-v1``, and ``pdb-atom-sorted-v1`` with the central
canonicalize registry. A claim that records its canonical form via
``result_canonical_form`` can then be re-canonicalised at replay time
by the same name.

The RDKit canonicaliser uses NFC string fallback when ``rdkit`` is
unavailable. The fallback is byte-stable but NOT chemically
canonical: tautomers and equivalent atom orderings of the same
molecule produce different fallback bytes. Verifiers should consult
:func:`rdkit_fallback_used` to decide whether byte equivalence on
this form alone is sufficient evidence.
"""

from __future__ import annotations

import unicodedata

from mareforma.canonicalize import register_canonicalizer


__all__ = [
    "HAS_RDKIT",
    "canonicalize_fasta_nfc_v1",
    "canonicalize_pdb_atom_sorted_v1",
    "canonicalize_rdkit_canonical_smiles_v1",
    "rdkit_fallback_used",
]


try:  # pragma: no cover — environment-dependent
    from rdkit import Chem  # type: ignore[import-not-found]
    HAS_RDKIT = True
except ImportError:
    HAS_RDKIT = False


def rdkit_fallback_used() -> bool:
    """Return True iff the RDKit canonicaliser is in fallback mode."""
    return not HAS_RDKIT


def canonicalize_rdkit_canonical_smiles_v1(value: str) -> bytes:
    """Canonical SMILES via RDKit; NFC string fallback when rdkit missing."""
    if not isinstance(value, str):
        raise TypeError("SMILES canonicaliser expects a string")
    if HAS_RDKIT:  # pragma: no cover — environment-dependent
        mol = Chem.MolFromSmiles(value)
        if mol is None:
            raise ValueError(f"rdkit could not parse SMILES {value!r}")
        return Chem.MolToSmiles(mol, canonical=True).encode("utf-8")
    return unicodedata.normalize("NFC", value).strip().encode("utf-8")


def canonicalize_fasta_nfc_v1(value: str) -> bytes:
    """NFC + uppercase + strip trailing whitespace, UTF-8 bytes."""
    if not isinstance(value, str):
        raise TypeError("FASTA canonicaliser expects a string")
    return unicodedata.normalize("NFC", value).upper().strip().encode("utf-8")


_SERIAL_WIDTH = 5
_HY36_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_HY36_UPPER_OFFSET = 10 ** _SERIAL_WIDTH - 10 * 36 ** (_SERIAL_WIDTH - 1)
_HY36_LOWER_OFFSET = 10 ** _SERIAL_WIDTH + 16 * 36 ** (_SERIAL_WIDTH - 1)


def _decode_atom_serial(field: str) -> int:
    """Decode a PDB serial field, decimal or hybrid-36.

    Files past 99999 atoms encode the serial in hybrid-36: the uppercase
    run starts at ``A0000`` (100000) and the lowercase run continues
    above it. Raises ``ValueError`` on anything else.
    """
    text = field.strip()
    try:
        return int(text)
    except ValueError:
        pass
    if (
        len(text) == _SERIAL_WIDTH
        and text.isascii()
        and text[0].isalpha()
        and (text.isupper() or text.islower())
        and all(char in _HY36_DIGITS for char in text.upper())
    ):
        offset = _HY36_UPPER_OFFSET if text.isupper() else _HY36_LOWER_OFFSET
        return int(text.upper(), 36) + offset
    raise ValueError(f"unreadable PDB atom serial {field!r}")


def _sort_atom_blocks(value: str, serial_of) -> bytes:
    """Sort each ATOM/HETATM block by ``serial_of(field)``; keep the rest.

    Shared by both PDB forms so they differ only in how a serial field is
    read, which is the whole difference between the two names.
    """
    if not isinstance(value, str):
        raise TypeError("PDB canonicaliser expects a string")
    lines = value.splitlines()
    result: list[str] = []
    atom_block: list[tuple[int, int, str]] = []

    def _flush_atom_block() -> None:
        atom_block.sort(key=lambda t: (t[0], t[1]))
        result.extend(line for _, _, line in atom_block)
        atom_block.clear()

    for idx, line in enumerate(lines):
        if line.startswith(("ATOM", "HETATM")):
            try:
                serial = serial_of(line[6:11])
            except ValueError as exc:
                raise ValueError(f"{exc} on line {idx + 1}: {line!r}") from None
            atom_block.append((serial, idx, line))
        else:
            _flush_atom_block()
            result.append(line)
    _flush_atom_block()
    return ("\n".join(result) + "\n").encode("utf-8")


register_canonicalizer(
    "rdkit-canonical-smiles-v1", canonicalize_rdkit_canonical_smiles_v1,
)
register_canonicalizer("fasta-nfc-v1", canonicalize_fasta_nfc_v1)
register_canonicalizer("pdb-atom-sorted-v1", canonicalize_pdb_atom_sorted_v1)
