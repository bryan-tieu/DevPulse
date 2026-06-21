"""Make the Spark job modules importable from the tests.

The silver job lives in `spark/` (not an installed package), so put that dir on
the import path. Resolved relative to this file, it works both inside the Spark
container (/opt/devpulse/spark) and on a host checkout.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
