"""
ADF Config Validation: Sourcepath Overlap Detection
=====================================================
Compares DropToRaw_config.csv and droptoraw_SB.csv to ensure no sourcepath
exists in both configs. Overlapping sourcepaths cause the manual (SB) pipeline
to fail because the event-based trigger already moved the files.

Usage:
    pytest tests/test_config_overlap.py -v --tb=short
"""

import csv
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Configuration – adjust these paths to match your ADF repo structure
# ---------------------------------------------------------------------------
# By default, the CSVs are expected one level above the tests/ folder.
# Override via environment variables if your repo layout differs.
_REPO_ROOT = Path(__file__).resolve().parent.parent

DROPTORAW_CONFIG_PATH = os.environ.get(
    "DROPTORAW_CONFIG_PATH",
    str(_REPO_ROOT / "DropToRaw_config.csv"),
)
DROPTORAW_SB_CONFIG_PATH = os.environ.get(
    "DROPTORAW_SB_CONFIG_PATH",
    str(_REPO_ROOT / "droptoraw_SB.csv"),
)


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


def _read_sourcepaths(csv_path: str, column_name: str = "sourcepath") -> set[str]:
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestConfigSourcepathOverlap:
    """Validate that DropToRaw_config.csv and droptoraw_SB.csv have no
    overlapping sourcepaths.

    If an overlap is detected, the event-based trigger (DropToRaw) will auto-
    move the file before the manual schedule-based (SB) pipeline runs, causing
    the SB pipeline to fail with nothing to load.
    """

    def test_no_overlapping_sourcepaths(self):
        """FAIL if any sourcepath from DropToRaw_config.csv also exists in
        droptoraw_SB.csv.
        """
        event_paths = _read_sourcepaths(DROPTORAW_CONFIG_PATH)
        manual_paths = _read_sourcepaths(DROPTORAW_SB_CONFIG_PATH)

        overlap = event_paths & manual_paths

        assert not overlap, (
            f"\n{'=' * 70}\n"
            f"  CONFIG OVERLAP DETECTED — CI/CD BLOCKED\n"
            f"{'=' * 70}\n\n"
            f"  {len(overlap)} sourcepath(s) exist in BOTH configs.\n"
            f"  These paths are auto-triggered by the event-based pipeline\n"
            f"  (DropToRaw_config.csv) and should NOT also be in the manual\n"
            f"  schedule-based config (droptoraw_SB.csv).\n\n"
            f"  Overlapping sourcepaths:\n"
            + "\n".join(f"    • {p}" for p in sorted(overlap))
            + f"\n\n"
            f"  FIX: Remove the above path(s) from droptoraw_SB.csv\n"
            f"  (they are already handled by the event-based trigger).\n"
            f"{'=' * 70}\n"
        )

    def test_droptoraw_config_not_empty(self):
        """Ensure DropToRaw_config.csv has at least one record."""
        paths = _read_sourcepaths(DROPTORAW_CONFIG_PATH)
        assert len(paths) > 0, "DropToRaw_config.csv has no sourcepath records."

    def test_droptoraw_sb_config_not_empty(self):
        """Ensure droptoraw_SB.csv has at least one record."""
        paths = _read_sourcepaths(DROPTORAW_SB_CONFIG_PATH)
        assert len(paths) > 0, "droptoraw_SB.csv has no sourcepath records."

    def test_no_duplicate_sourcepaths_in_droptoraw(self):
        """Ensure DropToRaw_config.csv has no internal duplicate sourcepaths."""
        csv_path = DROPTORAW_CONFIG_PATH
        all_paths = []

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            col_map = {name.strip().lower(): name for name in reader.fieldnames}
            actual_col = col_map.get("sourcepath")
            if actual_col:
                for row in reader:
                    val = row.get(actual_col, "").strip()
                    if val:
                        all_paths.append(_normalize_path(val))

        seen = set()
        duplicates = set()
        for p in all_paths:
            if p in seen:
                duplicates.add(p)
            seen.add(p)

        assert not duplicates, (
            f"Duplicate sourcepaths found in DropToRaw_config.csv:\n"
            + "\n".join(f"  • {d}" for d in sorted(duplicates))
        )

    def test_no_duplicate_sourcepaths_in_sb(self):
        """Ensure droptoraw_SB.csv has no internal duplicate sourcepaths."""
        csv_path = DROPTORAW_SB_CONFIG_PATH
        all_paths = []

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            col_map = {name.strip().lower(): name for name in reader.fieldnames}
            actual_col = col_map.get("sourcepath")
            if actual_col:
                for row in reader:
                    val = row.get(actual_col, "").strip()
                    if val:
                        all_paths.append(_normalize_path(val))

        seen = set()
        duplicates = set()
        for p in all_paths:
            if p in seen:
                duplicates.add(p)
            seen.add(p)

        assert not duplicates, (
            f"Duplicate sourcepaths found in droptoraw_SB.csv:\n"
            + "\n".join(f"  • {d}" for d in sorted(duplicates))
        )
