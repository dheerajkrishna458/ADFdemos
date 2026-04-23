# Databricks notebook source

# MAGIC %md
# MAGIC # Medallion Layer Unit Tests
# MAGIC
# MAGIC Runs all pytest unit tests for the **Bronze**, **Silver**, and **Gold** layer classes.

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

try:
    dbutils.library.restartPython()
except NameError:
    pass

# COMMAND ----------

import sys
import os
import subprocess

# ─────────────────────────────────────────────────────────────────────────
# SET THESE PATHS MANUALLY
# ─────────────────────────────────────────────────────────────────────────

SRC_DIR  = "/Workspace/Users/<your-email>/<your-repo>/core_utils/src"
TEST_DIR = "/Workspace/Users/<your-email>/<your-repo>/core_utils/src/tests/layers"

print(f"Source dir:  {SRC_DIR}")
print(f"Test dir:    {TEST_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Tests

# COMMAND ----------

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        TEST_DIR,
        "-v",
        "--tb=short",
        "--no-header",
    ],
    cwd=SRC_DIR,
    capture_output=True,
    text=True,
)

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
    print("[PASSED] ALL TESTS PASSED")
else:
    print(f"[FAILED] TESTS FAILED (exit code: {result.returncode})")
print("=" * 70)
