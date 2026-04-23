# Databricks notebook source

# MAGIC %md
# MAGIC # Medallion Layer Unit Tests
# MAGIC
# MAGIC Runs all pytest unit tests for the **Bronze**, **Silver**, and **Gold** layer classes.
# MAGIC
# MAGIC All Spark/Databricks dependencies are mocked — no real pipeline execution occurs.
# MAGIC The test conftest.py handles all imports internally via stubs, so no manual
# MAGIC sys.path setup or framework imports are needed in this notebook.

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

# Restart Python after pip install to pick up the new package
try:
    dbutils.library.restartPython()
except NameError:
    pass  # Not running in Databricks

# COMMAND ----------

import sys
import os
import subprocess

# ─────────────────────────────────────────────────────────────────────────
# Locate the test directory
# ─────────────────────────────────────────────────────────────────────────
# The conftest.py uses Path(__file__) to find framework/layers/ relative
# to itself, so we only need to find where the test files are.

# ── Auto-detect from notebook location ──
try:
    notebook_path = (
        dbutils.notebook.entry_point
        .getDbutils().notebook().getContext()
        .notebookPath().get()
    )
    workspace_prefix = "/Workspace"
    search_dir = os.path.dirname(f"{workspace_prefix}{notebook_path}")

    # Walk up directory tree until we find core_utils/src
    for _ in range(10):
        candidate_src = os.path.join(search_dir, "core_utils", "src")
        if os.path.isdir(candidate_src):
            SRC_DIR = candidate_src
            break
        search_dir = os.path.dirname(search_dir)
    else:
        SRC_DIR = None

    if SRC_DIR is None:
        raise FileNotFoundError("Could not auto-detect core_utils/src")
    print(f"[AUTO-DETECTED] SRC_DIR = {SRC_DIR}")

except Exception as e:
    # ── Fallback: set path manually ──
    # EDIT the path below to match your workspace layout
    print(f"[INFO] Auto-detect skipped ({e}), using manual path")
    SRC_DIR = os.path.join(os.getcwd(), "core_utils", "src")

TEST_DIR = os.path.join(SRC_DIR, "tests", "layers")

print(f"Source dir:  {SRC_DIR}")
print(f"Test dir:    {TEST_DIR}")
print()

# Verify the test files exist
for expected in ["conftest.py", "test_bronze_ingestor.py", "test_silver_refiner.py", "test_gold_aggregator.py"]:
    fpath = os.path.join(TEST_DIR, expected)
    status = "OK" if os.path.isfile(fpath) else "MISSING"
    print(f"  [{status}] {expected}")

assert os.path.isdir(TEST_DIR), f"Test directory not found: {TEST_DIR}"

# Also verify the layer source files exist (conftest loads these via importlib)
LAYER_BASE = os.path.join(SRC_DIR, "framework", "layers")
for layer_file in ["bronze/bronze_ingestor.py", "silver/silver_refiner.py", "gold/gold_aggregator.py"]:
    fpath = os.path.join(LAYER_BASE, layer_file)
    status = "OK" if os.path.isfile(fpath) else "MISSING"
    print(f"  [{status}] framework/layers/{layer_file}")

print()
print("[OK] All checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Tests

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────
# Execute pytest via subprocess
# ─────────────────────────────────────────────────────────────────────────
# No sys.path manipulation needed — conftest.py handles all imports
# internally using sys.modules stubs + importlib.util.

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        TEST_DIR,
        "-v",                    # Verbose: show each test name
        "--tb=short",            # Short tracebacks on failure
        "--no-header",           # Skip pytest header
    ],
    cwd=SRC_DIR,                 # Working directory = core_utils/src
    capture_output=True,
    text=True,
)

# ─────────────────────────────────────────────────────────────────────────
# Display results
# ─────────────────────────────────────────────────────────────────────────

print("=" * 70)
print("PYTEST OUTPUT")
print("=" * 70)
print(result.stdout)

if result.stderr:
    print("-" * 70)
    print("STDERR")
    print("-" * 70)
    print(result.stderr)

print("=" * 70)
if result.returncode == 0:
    print("[PASSED] ALL 37 TESTS PASSED")
else:
    print(f"[FAILED] TESTS FAILED (exit code: {result.returncode})")
print("=" * 70)
