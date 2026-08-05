"""A package that seizes a predicate URI just by being imported.

The adapter import-hygiene guards assert that importing an adapter
registers nothing. This fixture is the counter-example they are measured
against: if a guard reports zero registrations for this package, the
guard is a no-op and would miss a real adapter doing the same thing.
"""
from __future__ import annotations

from mareforma.predicate_types import register

POLLUTE_DEMO_V1 = "urn:mareforma:predicate:pollute-demo:v1"

register(POLLUTE_DEMO_V1, owner="tests.fixtures.predicate_polluter")
