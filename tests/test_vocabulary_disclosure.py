"""Disclosure of the retired support ladder at the read surfaces.

The support ladder (PRELIMINARY / REPLICATED / ESTABLISHED) is retired and
removed in v0.4.0. Because the column is NOT NULL DEFAULT 'PRELIMINARY', a
project that never named a level still stores one, so no call site warns on the
commonest path. ``health()`` and ``mareforma status`` disclose the retirement
where the value is served, and the ``min_support`` rejection names it too.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma.cli import cli
from mareforma.db import open_db, query_claims, search_claims
from mareforma.health import compute_health


