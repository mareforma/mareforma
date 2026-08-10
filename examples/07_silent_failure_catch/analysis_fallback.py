"""analysis_fallback.py: the same finding, silently unbacked.

The measured table moved and the read now fails, so the pipeline falls back to a
baked-in prior and reports a number that looks identical to the honest run. The
transcript cannot tell the two apart. The grounding observer can:

    python analysis_fallback.py
    mareforma diagnose --cites data.csv -- analysis_fallback.py

data.csv is cited but never opened, so the finding is UNGROUNDED: the catch this
example exists to show.
"""

import pandas as pd

try:
    frame = pd.read_csv("measurements.csv")
    knockdown = frame[frame["sample_id"].str.startswith("BRCA1_kd")]["expression"].mean()
    control = frame[frame["sample_id"].str.startswith("control")]["expression"].mean()
    ratio = knockdown / control
except FileNotFoundError:
    # Silent fallback: a plausible prior, no data read. This is the failure the
    # observer catches, because nothing here opens data.csv.
    ratio = 0.41

print(f"BRCA1 knockdown / control expression ratio: {ratio:.3f}")
