"""analysis.py: the honest pipeline.

Reads the measured expression table and reports the BRCA1-knockdown to control
ratio. Run it directly, or under the grounding observer to confirm the cited
data actually flowed into the number:

    python analysis.py
    mareforma diagnose --cites data.csv -- analysis.py

The observer sees the read of data.csv, so the finding is GROUNDED.
"""

import pandas as pd

frame = pd.read_csv("data.csv")
knockdown = frame[frame["sample_id"].str.startswith("BRCA1_kd")]["expression"].mean()
control = frame[frame["sample_id"].str.startswith("control")]["expression"].mean()
ratio = knockdown / control

print(f"BRCA1 knockdown / control expression ratio: {ratio:.3f}")
