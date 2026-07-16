"""Classification normalizes each read once, not once per read per pass.

``normalize_identifier`` reaches ``os.path.realpath`` (a filesystem syscall). A
target that reads in a per-record loop retains one ``ReadRecord`` per read, and a
naive ``classify`` re-normalizes every one of them several times per pass, then
the post-hoc auditor re-runs the whole pass per finding. The result is O(reads x
findings x cited) syscalls over evidence that never changes. The normalized form
is computed once per read and reused across the match scan, the read-set rebuild,
and every per-finding pass.
"""
from __future__ import annotations

import os.path

import pytest

from mareforma.observe import _citation, _scope

_READ = "/data/incidental.csv"  # a read that does NOT match the citation
_CITED = "/data/cited.csv"      # forces the full non-matching scan, not early exit


@pytest.fixture
def count_realpath(monkeypatch):
    real = os.path.realpath
    calls: list[str] = []

    def counting(p):
        calls.append(p)
        return real(p)

    monkeypatch.setattr(os.path, "realpath", counting)
    return calls


def test_single_classify_normalizes_each_read_once(count_realpath):
    cited = (_citation.normalize_identifier(_CITED),)
    scope = _scope.Scope(cited=cited)
    for _ in range(50):
        scope.record_read("file", _READ, True)
    count_realpath.clear()  # ignore setup normalization
    scope.classify()
    # A full non-matching scan over 50 identical reads must realpath the read a
    # small constant number of times, not once per read across every sub-pass.
    assert count_realpath.count(_READ) <= 2


def test_audit_reuses_normalization_across_findings(count_realpath):
    cited = (_citation.normalize_identifier(_CITED),)
    scope = _scope.Scope(cited=cited)
    for _ in range(20):
        scope.record_read("file", _READ, True)
    scope.classify()  # warm the shared normalization
    count_realpath.clear()
    for _ in range(10):  # ten findings over the same shared evidence
        scope.classify_against(cited)
    # The reads never change between findings, so no finding re-normalizes them.
    assert count_realpath.count(_READ) == 0
