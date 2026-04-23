# Databricks notebook source

# MAGIC %md
# MAGIC # Medallion Layer Unit Tests
# MAGIC
# MAGIC Runs all pytest unit tests for the **Bronze**, **Silver**, and **Gold** layer classes.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - This notebook should be in the same Databricks Repo as the framework code
# MAGIC - The `core_utils/src` directory must be on the Python path
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Installs pytest
# MAGIC 2. Adds the `core_utils/src` directory to `sys.path` (so `framework.*` imports resolve)
# MAGIC 3. Discovers and runs all tests under `tests/layers/`
# MAGIC 4. Displays pass/fail results with detailed output

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

# Restart Python after pip install to pick up the new package
dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
import subprocess

# ─────────────────────────────────────────────────────────────────────────
# Resolve paths relative to this notebook's location in the Repo
# ─────────────────────────────────────────────────────────────────────────

# Get the directory this notebook lives in
# Adjust REPO_ROOT based on where you place this notebook:
#   - If notebook is at repo root:        REPO_ROOT = os.getcwd()
#   - If notebook is at core_utils/src/:  REPO_ROOT = os.path.dirname(os.getcwd()) (go up)
#
# For DABs, the repo is typically mounted at:
#   /Workspace/Repos/<user>/<repo_name>/
# or
#   /Workspace/Users/<user>/<repo_name>/

# ── Option 1: Auto-detect from notebook path (recommended) ──
try:
    # In Databricks, get the notebook path from the context
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # The repo root is typically 2 levels up from the notebook:
    #   /Repos/<user>/<repo>/run_layer_tests  → /Repos/<user>/<repo>
    workspace_prefix = "/Workspace"
    repo_root = os.path.dirname(f"{workspace_prefix}{notebook_path}")
    print(f"📓 Notebook path: {notebook_path}")
except Exception:
    # Fallback: assume CWD is the repo root
    repo_root = os.getcwd()

print(f"📂 Repo root:     {repo_root}")

# The src directory containing the framework package and tests
SRC_DIR = os.path.join(repo_root, "core_utils", "src")
TEST_DIR = os.path.join(SRC_DIR, "tests", "layers")

print(f"📦 Source dir:    {SRC_DIR}")
print(f"🧪 Test dir:      {TEST_DIR}")

# Verify paths exist
assert os.path.isdir(SRC_DIR), f"❌ Source directory not found: {SRC_DIR}"
assert os.path.isdir(TEST_DIR), f"❌ Test directory not found: {TEST_DIR}"
print("✅ All paths verified")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────
# Add src to sys.path so 'framework.*' imports resolve
# ─────────────────────────────────────────────────────────────────────────

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
    print(f"✅ Added {SRC_DIR} to sys.path")
else:
    print(f"ℹ️  {SRC_DIR} already on sys.path")

# Quick sanity check — verify the framework is importable
try:
    from framework.layers.bronze.bronze_ingestor import BronzeIngester
    from framework.layers.silver.silver_refiner import SilverRefiner
    from framework.layers.gold.gold_aggregator import GoldAggregator
    print("✅ All layer modules imported successfully")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print("   Check that core_utils/src/framework/ has __init__.py files")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Tests

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────
# Execute pytest via subprocess (cleanest approach for notebooks)
# ─────────────────────────────────────────────────────────────────────────

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        TEST_DIR,
        "-v",                    # Verbose: show each test name
        "--tb=short",            # Short tracebacks on failure
        "--no-header",           # Skip pytest header for cleaner output
        "-q",                    # Quiet: less boilerplate
    ],
    cwd=SRC_DIR,                 # Working directory = core_utils/src
    capture_output=True,
    text=True,
    env={**os.environ, "PYTHONPATH": SRC_DIR},  # Ensure framework is importable
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
    print("✅ ALL TESTS PASSED")
else:
    print(f"❌ TESTS FAILED (exit code: {result.returncode})")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Run with pytest API (inline, same process)
# MAGIC
# MAGIC Use this if subprocess has issues with path resolution.
# MAGIC Uncomment and run the cell below instead.

# COMMAND ----------

# import pytest
#
# # Run pytest in the same Python process
# exit_code = pytest.main([
#     TEST_DIR,
#     "-v",
#     "--tb=short",
#     "--no-header",
# ])
#
# if exit_code == 0:
#     print("✅ ALL TESTS PASSED")
# else:
#     print(f"❌ TESTS FAILED (exit code: {exit_code})")
