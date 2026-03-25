"""
mareforma.pipeline — DAG-based pipeline runner.

Public surface:
    from mareforma.pipeline import build
"""

from mareforma.pipeline.runner import TransformRunner, BuildResult
from mareforma.pipeline.dag import resolve, CyclicDependencyError, MissingDependencyError
from mareforma.pipeline.lock import PipelineLock
from mareforma.pipeline.context import BuildContext, ArtifactNotFoundError
from mareforma.pipeline.discovery import discover, DiscoveryError

__all__ = [
    "TransformRunner",
    "BuildResult",
    "resolve",
    "CyclicDependencyError",
    "MissingDependencyError",
    "PipelineLock",
    "BuildContext",
    "ArtifactNotFoundError",
    "discover",
    "DiscoveryError",
]