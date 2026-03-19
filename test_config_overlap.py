"""
ADF Config Validation: Sourcepath Overlap Detection
=====================================================
Compares DropToRaw_config.csv and droptoraw_SB.csv to ensure no sourcepath
exists in both configs — for EACH environment (dev, beta, prod).

Overlapping sourcepaths cause the manual (SB) pipeline to fail because the
event-based trigger already moved the files.

Usage:
    pytest tests/test_config_overlap.py -v --tb=short
"""

import csv
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# By default, config root is: <repo>/release-pipeline/config/
# Override via environment variable if needed.
_REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_ROOT = Path(
    os.environ.get(
        "CONFIG_ROOT",
        str(_REPO_ROOT / "release-pipeline" / "config"),
    )
)

# Environments to validate
ENVIRONMENTS = ["dev", "beta", "prod"]

# Config file names (expected inside each environment folder)
DROPTORAW_FILENAME = "DropToRaw_config.csv"
DROPTORAW_SB_FILENAME = "SB_DropToRaw_config.csv"

# Column name in both CSVs
SOURCE_PATH_COLUMN = "SourcePath"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_path(p: str) -> str:
    """Normalize a sourcepath for consistent comparison.

    - Strips whitespace
    - Converts backslashes to forward slashes
    - Lowercases (Azure Blob Storage paths are case-insensitive)
    - Strips trailing slashes
    """
    return p.strip().replace("\\", "/").lower().rstrip("/")


def _read_sourcepaths(csv_path: str, column_name: str = SOURCE_PATH_COLUMN) -> set[str]:
    """Read and return unique, normalized sourcepaths from a CSV config file.

    The column_name lookup is case-insensitive to handle header variations
    like 'SourcePath', 'sourcepath', 'SOURCEPATH', etc.
    """
    if not os.path.isfile(csv_path):
        pytest.fail(
            f"Config file not found: {csv_path}\n"
            "Ensure the file exists and the path is correct."
        )

    sourcepaths: set[str] = set()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # Case-insensitive column lookup
        if reader.fieldnames is None:
            pytest.fail(f"Config file is empty or has no headers: {csv_path}")

        col_map = {name.strip().lower(): name for name in reader.fieldnames}
        actual_col = col_map.get(column_name.lower())

        if actual_col is None:
            pytest.fail(
                f"Column '{column_name}' not found in {csv_path}.\n"
                f"Available columns: {list(reader.fieldnames)}"
            )

        for row_num, row in enumerate(reader, start=2):  # row 1 = header
            raw_value = row.get(actual_col, "").strip()
            if raw_value:
                sourcepaths.add(_normalize_path(raw_value))
            else:
                pytest.fail(
                    f"Empty sourcepath at row {row_num} in {csv_path}. "
                    "Every config row must have a sourcepath."
                )

    if not sourcepaths:
        pytest.fail(f"No sourcepath records found in {csv_path}.")

    return sourcepaths


def _read_all_sourcepaths(csv_path: str, column_name: str = SOURCE_PATH_COLUMN) -> list[str]:
    """Read ALL sourcepaths (including duplicates) for duplicate detection."""
    if not os.path.isfile(csv_path):
        pytest.fail(f"Config file not found: {csv_path}")

    all_paths = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            pytest.fail(f"Config file is empty or has no headers: {csv_path}")

        col_map = {name.strip().lower(): name for name in reader.fieldnames}
        actual_col = col_map.get(column_name.lower())

        if actual_col is None:
            return all_paths

        for row in reader:
            val = row.get(actual_col, "").strip()
            if val:
                all_paths.append(_normalize_path(val))

    return all_paths


def _get_config_path(env: str, filename: str) -> str:
    """Build the full path to a config file for a given environment."""
    return str(CONFIG_ROOT / env / filename)


# ---------------------------------------------------------------------------
# Tests — parametrized across dev, beta, prod
# ---------------------------------------------------------------------------
class TestConfigSourcepathOverlap:
    """Validate that DropToRaw_config.csv and droptoraw_SB.csv have no
    overlapping sourcepaths — for each environment.
    """

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_no_overlapping_sourcepaths(self, env):
        """FAIL if any sourcepath from DropToRaw_config.csv also exists in
        droptoraw_SB.csv for the given environment.
        """
        event_csv = _get_config_path(env, DROPTORAW_FILENAME)
        manual_csv = _get_config_path(env, DROPTORAW_SB_FILENAME)

        event_paths = _read_sourcepaths(event_csv)
        manual_paths = _read_sourcepaths(manual_csv)

        overlap = event_paths & manual_paths

        assert not overlap, (
            f"\n{'=' * 70}\n"
            f"  CONFIG OVERLAP DETECTED in [{env.upper()}] — CI/CD BLOCKED\n"
            f"{'=' * 70}\n\n"
            f"  {len(overlap)} sourcepath(s) exist in BOTH configs.\n"
            f"  These paths are auto-triggered by the event-based pipeline\n"
            f"  (DropToRaw_config.csv) and should NOT also be in the manual\n"
            f"  schedule-based config (droptoraw_SB.csv).\n\n"
            f"  Environment: {env}\n"
            f"  Event config:  {event_csv}\n"
            f"  Manual config: {manual_csv}\n\n"
            f"  Overlapping sourcepaths:\n"
            + "\n".join(f"    • {p}" for p in sorted(overlap))
            + f"\n\n"
            f"  FIX: Remove the above path(s) from {env}/{DROPTORAW_SB_FILENAME}\n"
            f"  (they are already handled by the event-based trigger).\n"
            f"{'=' * 70}\n"
        )

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_droptoraw_config_not_empty(self, env):
        """Ensure DropToRaw_config.csv has at least one record."""
        csv_path = _get_config_path(env, DROPTORAW_FILENAME)
        paths = _read_sourcepaths(csv_path)
        assert len(paths) > 0, (
            f"[{env.upper()}] {DROPTORAW_FILENAME} has no sourcepath records."
        )

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_droptoraw_sb_config_not_empty(self, env):
        """Ensure droptoraw_SB.csv has at least one record."""
        csv_path = _get_config_path(env, DROPTORAW_SB_FILENAME)
        paths = _read_sourcepaths(csv_path)
        assert len(paths) > 0, (
            f"[{env.upper()}] {DROPTORAW_SB_FILENAME} has no sourcepath records."
        )

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_no_duplicate_sourcepaths_in_droptoraw(self, env):
        """Ensure DropToRaw_config.csv has no internal duplicate sourcepaths."""
        csv_path = _get_config_path(env, DROPTORAW_FILENAME)
        all_paths = _read_all_sourcepaths(csv_path)

        seen = set()
        duplicates = set()
        for p in all_paths:
            if p in seen:
                duplicates.add(p)
            seen.add(p)

        assert not duplicates, (
            f"[{env.upper()}] Duplicate sourcepaths in {DROPTORAW_FILENAME}:\n"
            + "\n".join(f"  • {d}" for d in sorted(duplicates))
        )

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_no_duplicate_sourcepaths_in_sb(self, env):
        """Ensure droptoraw_SB.csv has no internal duplicate sourcepaths."""
        csv_path = _get_config_path(env, DROPTORAW_SB_FILENAME)
        all_paths = _read_all_sourcepaths(csv_path)

        seen = set()
        duplicates = set()
        for p in all_paths:
            if p in seen:
                duplicates.add(p)
            seen.add(p)

        assert not duplicates, (
            f"[{env.upper()}] Duplicate sourcepaths in {DROPTORAW_SB_FILENAME}:\n"
            + "\n".join(f"  • {d}" for d in sorted(duplicates))
        )
