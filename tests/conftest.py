import os
import sys
import time
from pathlib import Path

# Make collected timestamps unambiguous: PySpark converts to the process time zone.
os.environ["TZ"] = "UTC"
time.tzset()

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
