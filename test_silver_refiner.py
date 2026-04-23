"""
Pytest test suite for SilverRefiner.

Includes:
  - Function-level unit tests for __init__, transform(), load()
  - End-to-end tests for run() covering success, skip (None extract),
    error, and finally-block paths
"""

import types
import pytest
from unittest.mock import MagicMock, patch, call


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build a SilverRefiner with all dependencies mocked out
# ═══════════════════════════════════════════════════════════════════════════

def _build_silver_refiner(
    mock_args,
    mock_spark,
    mock_env_manager,
    mock_audit_logger,
    mock_delta_manager,
    mock_delta_handler,
    silver_config,
):
    """
    Instantiate a SilverRefiner while patching away every heavy dependency
    that normally runs during MedallionBase.__init__.
    """
    with patch("framework.layers.silver.silver_refiner.MedallionBase.__init__") as mock_base_init:
        mock_base_init.return_value = None

        from framework.layers.silver.silver_refiner import SilverRefiner

        refiner = SilverRefiner.__new__(SilverRefiner)

        # Wire up attributes that MedallionBase.__init__ would set
        refiner.args = mock_args
        refiner.spark = mock_spark
        refiner.env_manager = mock_env_manager
        refiner.audit_logger = mock_audit_logger
        refiner.delta_manager = mock_delta_manager
        refiner.delta_handler = mock_delta_handler
        refiner.config = silver_config
        refiner.source_config = silver_config.get("source", {})
        refiner.logger = MagicMock(name="Logger")

        # Replicate real __init__ body (minus super().__init__)
        refiner.pipeline_config = silver_config.get("pipeline_metadata", {})
        refiner.source_config = silver_config.get("source", {})
        refiner.transform_config = silver_config.get("transform", {})
        refiner.destination_config = silver_config.get("destination", {})

    return refiner


# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION-LEVEL UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverRefinerUnit:
    """Unit tests for individual SilverRefiner methods."""

    # ---- __init__ ---------------------------------------------------------

    def test_init_loads_config_sections(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
    ):
        """Verify __init__ correctly splits config into sub-dicts."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        assert refiner.pipeline_config == silver_config["pipeline_metadata"]
        assert refiner.source_config == silver_config["source"]
        assert refiner.transform_config == silver_config["transform"]
        assert refiner.destination_config == silver_config["destination"]

    # ---- transform() ------------------------------------------------------

    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_transform_calls_steps_in_order(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify transform() calls add_custom_columns, apply_standardize, apply_casts."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )

        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.apply_casts.return_value = mock_df

        result = refiner.transform(mock_df)

        mock_tu.add_custom_columns.assert_called_once_with(mock_df, refiner.transform_config)
        mock_tu.apply_standardize.assert_called_once_with(mock_spark, mock_df, refiner.transform_config)
        mock_tu.apply_casts.assert_called_once_with(mock_df, refiner.transform_config)
        assert result is mock_df

    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_transform_returns_dataframe(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify transform() returns a DataFrame."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.apply_casts.return_value = mock_df

        result = refiner.transform(mock_df)
        assert result is not None

    # ---- load() -----------------------------------------------------------

    def test_load_append_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify load() calls write_df with append mode and silver properties."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.destination_config["mode"] = "append"

        refiner.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "append",
            mock_delta_manager.silver_tbl_properties,
        )

    def test_load_overwrite_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify load() calls write_df with overwrite mode."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.destination_config["mode"] = "overwrite"

        refiner.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "overwrite",
            mock_delta_manager.silver_tbl_properties,
        )

    def test_load_merge_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify load() calls merge_df when mode is 'merge'."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.destination_config["mode"] = "merge"
        refiner.destination_config["merge_condition"] = "tgt.id = src.id"

        refiner.load(mock_df)

        mock_delta_manager.merge_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "tgt.id = src.id",
        )

    def test_load_no_target_table_raises(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """Verify load() raises ValueError when no target table is configured."""
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        mock_env_manager.construct_table_fqn.return_value = ""

        with pytest.raises(ValueError, match="No target table configured"):
            refiner.load(mock_df)


# ═══════════════════════════════════════════════════════════════════════════
# END-TO-END TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverRefinerE2E:
    """End-to-end tests for the full run() pipeline."""

    @patch("framework.layers.silver.silver_refiner.Masker")
    @patch("framework.layers.silver.silver_refiner.DataQualityCheck")
    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_run_success_full_pipeline(
        self, mock_tu, mock_dqc_cls, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """
        Verify the full happy-path: extract → transform → DQ → load → mask → audit.
        """
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.extract = MagicMock(return_value=mock_df)

        # Stub transform steps
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.apply_casts.return_value = mock_df

        # DQ returns the same df
        mock_dqc_cls.return_value.apply_checks.return_value = mock_df

        refiner.run()

        # Verify call sequence
        refiner.extract.assert_called_once()
        mock_tu.add_custom_columns.assert_called_once()
        mock_tu.apply_standardize.assert_called_once()
        mock_tu.apply_casts.assert_called_once()
        mock_dqc_cls.assert_called_once_with(
            mock_spark, silver_config, mock_env_manager, mock_audit_logger, mock_delta_manager,
        )
        mock_dqc_cls.return_value.apply_checks.assert_called_once_with(mock_df)
        mock_delta_manager.write_df.assert_called_once()
        mock_masker_cls.assert_called_once()
        mock_masker_cls.return_value.apply_masking.assert_called_once()
        mock_audit_logger.log_run_audit.assert_called_once()
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_run_extract_returns_none_logs_audit_and_returns(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config,
    ):
        """
        When extract() returns None, run should log a success audit
        and skip transform/DQ/load/mask.
        """
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.extract = MagicMock(return_value=None)

        refiner.run()

        refiner.extract.assert_called_once()
        # Audit logged for "BronzeToSilver" success
        mock_audit_logger.log_run_audit.assert_called_once()
        # Transform NOT called
        mock_tu.add_custom_columns.assert_not_called()
        # Load NOT called
        mock_delta_manager.write_df.assert_not_called()
        # Finally block
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_run_exception_logs_error_and_reraises(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """
        When an exception occurs during transform, verify error logging,
        failure audit, and re-raise.
        """
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.extract = MagicMock(return_value=mock_df)

        # Force add_custom_columns to blow up
        mock_tu.add_custom_columns.side_effect = RuntimeError("Silver transform error!")

        with pytest.raises(RuntimeError, match="Silver transform error!"):
            refiner.run()

        refiner.logger.error.assert_called_once()
        mock_audit_logger.log_error.assert_called_once()
        # Failure audit
        mock_audit_logger.log_run_audit.assert_called_once()
        audit_call_args = mock_audit_logger.log_run_audit.call_args
        # First positional arg should be None (no df on failure)
        assert audit_call_args[0][0] is None
        # Should contain "failure"
        assert "failure" in audit_call_args[0]

    @patch("framework.layers.silver.silver_refiner.Masker")
    @patch("framework.layers.silver.silver_refiner.DataQualityCheck")
    @patch("framework.layers.silver.silver_refiner.TransformationUtils")
    def test_run_always_calls_finally_block(
        self, mock_tu, mock_dqc_cls, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        silver_config, mock_df,
    ):
        """
        Verify log_run_summary() and delta_handler.flush() are called
        regardless of success or failure.
        """
        refiner = _build_silver_refiner(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, silver_config,
        )
        refiner.extract = MagicMock(return_value=mock_df)

        # Stub transform
        mock_tu.add_custom_columns.return_value = mock_df
        mock_tu.apply_standardize.return_value = mock_df
        mock_tu.apply_casts.return_value = mock_df
        mock_dqc_cls.return_value.apply_checks.return_value = mock_df

        # --- Success path ---
        refiner.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1

        # Reset
        mock_audit_logger.log_run_summary.reset_mock()
        mock_delta_handler.flush.reset_mock()
        mock_audit_logger.log_run_audit.reset_mock()
        mock_audit_logger.log_error.reset_mock()

        # --- Failure path ---
        mock_tu.add_custom_columns.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            refiner.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1
