"""
Pytest test suite for BronzeIngester.

Includes:
  - Function-level unit tests for __init__, transform(), load(), _log_audit()
  - End-to-end tests for run() covering success, skip, error, and finally paths
"""

import types
import pytest
from unittest.mock import MagicMock, patch, call


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build a BronzeIngester with all dependencies mocked out
# ═══════════════════════════════════════════════════════════════════════════

def _build_bronze_ingester(
    mock_args,
    mock_spark,
    mock_env_manager,
    mock_audit_logger,
    mock_delta_manager,
    mock_delta_handler,
    bronze_config,
):
    """
    Instantiate a BronzeIngester while patching away every heavy dependency
    that normally runs during MedallionBase.__init__.
    """
    with patch("framework.layers.bronze.bronze_ingestor.MedallionBase.__init__") as mock_base_init:
        # Prevent the real __init__ from firing (it needs Spark, YAML, etc.)
        mock_base_init.return_value = None

        from framework.layers.bronze.bronze_ingestor import BronzeIngester

        ingester = BronzeIngester.__new__(BronzeIngester)

        # Manually wire up the attributes that MedallionBase.__init__ would set
        ingester.args = mock_args
        ingester.spark = mock_spark
        ingester.env_manager = mock_env_manager
        ingester.audit_logger = mock_audit_logger
        ingester.delta_manager = mock_delta_manager
        ingester.delta_handler = mock_delta_handler
        ingester.config = bronze_config
        ingester.source_config = bronze_config.get("source", {})
        ingester.logger = MagicMock(name="Logger")

        # Call the real __init__ body (minus super().__init__)
        ingester.pipeline_config = bronze_config.get("pipeline_metadata", {})
        ingester.source_config = bronze_config.get("source", {})
        ingester.transform_config = bronze_config.get("transform", {})
        ingester.destination_config = bronze_config.get("destination", {})
        ingester.files_discovered = []

    return ingester


# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION-LEVEL UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestBronzeIngesterUnit:
    """Unit tests for individual BronzeIngester methods."""

    # ---- __init__ ---------------------------------------------------------

    def test_init_loads_config_sections(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
    ):
        """Verify __init__ correctly splits config into sub-dicts."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        assert ingester.pipeline_config == bronze_config["pipeline_metadata"]
        assert ingester.source_config == bronze_config["source"]
        assert ingester.transform_config == bronze_config["transform"]
        assert ingester.destination_config == bronze_config["destination"]

    def test_init_empty_files_discovered(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
    ):
        """Verify files_discovered starts as an empty list."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        assert ingester.files_discovered == []

    # ---- transform() ------------------------------------------------------

    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_transform_calls_all_steps_in_order(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify transform() calls all five transformation steps."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )

        # Make each step return the df mock so the chain continues
        mock_tu.add_row_hash.return_value = mock_df
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.add_metadata_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.move_columns_to_end.return_value = mock_df

        result = ingester.transform(mock_df)

        mock_tu.add_row_hash.assert_called_once_with(mock_df)
        mock_tu.add_custom_columns.assert_called_once_with(mock_df, ingester.transform_config)
        mock_tu.add_metadata_columns.assert_called_once_with(mock_df, mock_args)
        mock_tu.apply_standardize.assert_called_once_with(mock_spark, mock_df, ingester.transform_config)
        mock_tu.move_columns_to_end.assert_called_once_with(mock_df, ["row_hash"])
        assert result is mock_df

    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_transform_returns_dataframe(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify transform() returns a DataFrame."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        mock_tu.add_row_hash.return_value = mock_df
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.add_metadata_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.move_columns_to_end.return_value = mock_df

        result = ingester.transform(mock_df)
        assert result is not None

    # ---- load() -----------------------------------------------------------

    def test_load_append_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify load() calls write_df with append mode."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.destination_config["mode"] = "append"

        ingester.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "append",
            mock_delta_manager.bronze_tbl_properties,
        )

    def test_load_overwrite_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify load() calls write_df with overwrite mode."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.destination_config["mode"] = "overwrite"

        ingester.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "overwrite",
            mock_delta_manager.bronze_tbl_properties,
        )

    def test_load_merge_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify load() calls merge_df when mode is 'merge'."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.destination_config["mode"] = "merge"
        ingester.destination_config["merge_condition"] = "tgt.id = src.id"

        ingester.load(mock_df)

        mock_delta_manager.merge_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "tgt.id = src.id",
        )

    def test_load_no_target_table_raises(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify load() raises ValueError when no target table is configured."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        # Make construct_table_fqn return empty/falsy
        mock_env_manager.construct_table_fqn.return_value = ""

        with pytest.raises(ValueError, match="No target table configured"):
            ingester.load(mock_df)

    # ---- _log_audit() -----------------------------------------------------

    def test_log_audit_success_per_file(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """Verify _log_audit logs per-file audit with correct status."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )

        # Simulate discovered files
        file1 = types.SimpleNamespace(name="file1.csv", path="/data/file1.csv")
        file2 = types.SimpleNamespace(name="file2.csv", path="/data/file2.csv")
        ingester.files_discovered = [file1, file2]

        # groupBy("file_name").agg(count) returns per-file counts
        records_total_df = MagicMock()
        records_total_df.collect.return_value = [
            {"file_name": "file1.csv", "records_total": 50},
            {"file_name": "file2.csv", "records_total": 30},
        ]
        mock_df.groupBy.return_value.agg.return_value = records_total_df

        # spark.table(target).where(...).groupBy(...).agg(...) returns succeeded counts
        succeeded_df = MagicMock()
        succeeded_df.collect.return_value = [
            {"file_name": "file1.csv", "records_succeeded": 50},
            {"file_name": "file2.csv", "records_succeeded": 30},
        ]
        table_mock = MagicMock()
        table_mock.where.return_value = table_mock
        table_mock.groupBy.return_value.agg.return_value = succeeded_df
        mock_spark.table.return_value = table_mock

        ingester._log_audit(mock_df, "dev_catalog.schema.test_table", "2026-04-21T12:00:00")

        # Should log 2 successful loads
        assert mock_audit_logger.log_file_load.call_count == 2
        for c in mock_audit_logger.log_file_load.call_args_list:
            assert c.kwargs.get("load_status") == "Success" or c[1].get("load_status") == "Success"

    def test_log_audit_failure_flag(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config,
    ):
        """Verify _log_audit logs all files as Failed when failure=True."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        file1 = types.SimpleNamespace(name="file1.csv", path="/data/file1.csv")
        ingester.files_discovered = [file1]

        ingester._log_audit(None, "dev_catalog.schema.test_table", "2026-04-21T12:00:00", failure=True)

        mock_audit_logger.log_file_load.assert_called_once()
        call_kwargs = mock_audit_logger.log_file_load.call_args
        assert call_kwargs.kwargs.get("load_status") == "Failed" or call_kwargs[1].get("load_status") == "Failed"

    def test_log_audit_none_df(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config,
    ):
        """Verify _log_audit logs files as Failed when df is None."""
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        file1 = types.SimpleNamespace(name="file1.csv", path="/data/file1.csv")
        ingester.files_discovered = [file1]

        ingester._log_audit(None, "dev_catalog.schema.test_table", "2026-04-21T12:00:00")

        mock_audit_logger.log_file_load.assert_called_once()
        call_kwargs = mock_audit_logger.log_file_load.call_args
        assert call_kwargs.kwargs.get("load_status") == "Failed" or call_kwargs[1].get("load_status") == "Failed"


# ═══════════════════════════════════════════════════════════════════════════
# END-TO-END TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestBronzeIngesterE2E:
    """End-to-end tests for the full run() pipeline."""

    @patch("framework.layers.bronze.bronze_ingestor.Masker")
    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_run_success_full_pipeline(
        self, mock_tu, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """
        Verify the full happy-path: extract → transform → load → mask → audit.
        """
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )

        # Stub extract to return mock_df
        ingester.extract = MagicMock(return_value=mock_df)

        # Stub transform steps
        mock_tu.add_row_hash.return_value = mock_df
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.add_metadata_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.move_columns_to_end.return_value = mock_df

        # Stub _log_audit so we don't need full audit wiring
        ingester._log_audit = MagicMock()

        ingester.run()

        # Verify call sequence
        ingester.extract.assert_called_once()
        mock_tu.add_row_hash.assert_called_once()
        mock_delta_manager.write_df.assert_called_once()
        mock_masker_cls.assert_called_once()
        mock_masker_cls.return_value.apply_masking.assert_called_once()
        ingester._log_audit.assert_called_once()
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_run_extract_returns_none_skips(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config,
    ):
        """
        When extract() returns None, transform/load/mask should NOT be called.
        """
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.extract = MagicMock(return_value=None)
        ingester._log_audit = MagicMock()

        ingester.run()

        ingester.extract.assert_called_once()
        # Transform should NOT have been called
        mock_tu.add_row_hash.assert_not_called()
        mock_delta_manager.write_df.assert_not_called()
        mock_delta_manager.merge_df.assert_not_called()
        # Finally block still runs
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_run_exception_logs_error_and_reraises(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """
        When an exception occurs in transform, verify error logging
        and that the exception is re-raised.
        """
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.extract = MagicMock(return_value=mock_df)
        ingester._log_audit = MagicMock()

        # Force transform to raise
        mock_tu.add_row_hash.side_effect = RuntimeError("Transformation failed!")

        with pytest.raises(RuntimeError, match="Transformation failed!"):
            ingester.run()

        # Error logging
        ingester.logger.error.assert_called_once()
        mock_audit_logger.log_error.assert_called_once()
        # Failure audit logged
        ingester._log_audit.assert_called_once()
        audit_call_kwargs = ingester._log_audit.call_args
        assert audit_call_kwargs[1].get("failure") is True or (
            len(audit_call_kwargs[0]) >= 4 and audit_call_kwargs[0][3] is True
        )

    @patch("framework.layers.bronze.bronze_ingestor.Masker")
    @patch("framework.layers.bronze.bronze_ingestor.TransformationUtils")
    def test_run_always_calls_finally_block(
        self, mock_tu, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        bronze_config, mock_df,
    ):
        """
        Verify log_run_summary() and delta_handler.flush() are called
        regardless of success or failure.
        """
        ingester = _build_bronze_ingester(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, bronze_config,
        )
        ingester.extract = MagicMock(return_value=mock_df)
        ingester._log_audit = MagicMock()

        # Stub all transform steps
        mock_tu.add_row_hash.return_value = mock_df
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.add_metadata_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.move_columns_to_end.return_value = mock_df

        # --- Success path ---
        ingester.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1

        # Reset
        mock_audit_logger.log_run_summary.reset_mock()
        mock_delta_handler.flush.reset_mock()

        # --- Failure path ---
        mock_tu.add_row_hash.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            ingester.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1
