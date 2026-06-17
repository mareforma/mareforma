"""mareforma.trust, the trust layer (proposition + evidence + status).

This package makes AI-generated findings *comparable and trustworthy*: a
content-addressed :class:`Proposition` is the unit of sameness, a pre-registered
:class:`Prediction` turns an outcome into a computed :class:`Bearing` (never a
declared one), and a count-based :class:`Status` derives trust from independent
lines of evidence with no human in the loop and no ML.

It is built strictly on top of the signed claim graph: every finding rides a
signed claim as its attestation (who asserted it, when), while these objects
carry the structured meaning. The identity hash (``content_id`` / ``frame_id``)
and the Proposition field set are the frozen kernel; Status is a versioned
policy over the same stored data. See the design doc for the full north-star
model that this layer implements a strict prefix of.
"""
from __future__ import annotations

from .bearing import Bearing, BearingDirection, compute_bearing
from .errors import (
    FindingPlanForkError,
    InconsistentEstimateError,
    NoRegisteredPlanError,
    NonFalsifiablePropositionError,
    TrustError,
)
from .estimate import (
    Contrast,
    ControlType,
    EffectEstimate,
    EffectType,
    EvidenceLine,
    Scale,
    null_value,
)
from .gates import Gate, evaluate_gates, gates_for
from .prediction import (
    DirectionOfInterest,
    InferenceRegime,
    Prediction,
    TestType,
)
from .proposition import (
    REGISTRABLE_DIRECTIONS,
    Direction,
    Proposition,
    normalize_token,
)
from .status import (
    STATUS_POLICY,
    FrameStatus,
    Status,
    compute_frame_status,
    compute_status,
)

__all__ = [
    # proposition / identity
    "Proposition",
    "Direction",
    "REGISTRABLE_DIRECTIONS",
    "normalize_token",
    # prediction
    "Prediction",
    "TestType",
    "DirectionOfInterest",
    "InferenceRegime",
    # estimate / evidence tree
    "EffectEstimate",
    "EffectType",
    "Scale",
    "Contrast",
    "ControlType",
    "EvidenceLine",
    "null_value",
    # bearing / gate
    "Bearing",
    "BearingDirection",
    "compute_bearing",
    # decision-rule gates[] chain
    "Gate",
    "gates_for",
    "evaluate_gates",
    # status
    "Status",
    "FrameStatus",
    "compute_status",
    "compute_frame_status",
    "STATUS_POLICY",
    # errors
    "TrustError",
    "NonFalsifiablePropositionError",
    "NoRegisteredPlanError",
    "FindingPlanForkError",
    "InconsistentEstimateError",
]
