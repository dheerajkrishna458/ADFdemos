"""
Pytest test suite for GoldAggregator.

Includes:
  - Function-level unit tests for __init__, transform(), load()
  - End-to-end tests for run() covering success, skip (None extract),
    error, and finally-block paths
"""

import types
import pytest
from unittest.mock import MagicMock, patch, call


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build a GoldAggregator with all dependencies mocked out
# ═══════════════════════════════════════════════════════════════════════════

def _build_gold_aggregator(
    mock_args,
    mock_spark,
    mock_env_manager,
    mock_audit_logger,
    mock_delta_manager,
    mock_delta_handler,
    gold_config,
):
    """
    Instantiate a GoldAggregator while patching away every heavy dependency
    that normally runs during MedallionBase.__init__.
    """
    with patch("framework.layers.gold.gold_aggregator.MedallionBase.__init__") as mock_base_init:
        mock_base_init.return_value = None

        from framework.layers.gold.gold_aggregator import GoldAggregator

        aggregator = GoldAggregator.__new__(GoldAggregator)

        # Wire up attributes that MedallionBase.__init__ would set
        aggregator.args = mock_args
        aggregator.spark = mock_spark
        aggregator.env_manager = mock_env_manager
        aggregator.audit_logger = mock_audit_logger
        aggregator.delta_manager = mock_delta_manager
        aggregator.delta_handler = mock_delta_handler
        aggregator.config = gold_config
        aggregator.source_config = gold_config.get("source", {})
        aggregator.logger = MagicMock(name="Logger")

        # Replicate real __init__ body (minus super().__init__)
        aggregator.pipeline_config = gold_config.get("pipeline_metadata", {})
        aggregator.source_config = gold_config.get("source", {})
        aggregator.transform_config = gold_config.get("transform", {})
        aggregator.destination_config = gold_config.get("destination", {})

    return aggregator


# ═══════════════════════════════════════════════════════════════════════════
# FUNCTION-LEVEL UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestGoldAggregatorUnit:
    """Unit tests for individual GoldAggregator methods."""

    # ---- __init__ ---------------------------------------------------------

    def test_init_loads_config_sections(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
    ):
        """Verify __init__ correctly splits config into sub-dicts."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        assert aggregator.pipeline_config == gold_config["pipeline_metadata"]
        assert aggregator.source_config == gold_config["source"]
        assert aggregator.transform_config == gold_config["transform"]
        assert aggregator.destination_config == gold_config["destination"]

    # ---- transform() ------------------------------------------------------

    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_transform_calls_aggregations(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify transform() calls apply_aggregations with the correct args."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )

        mock_tu.apply_aggregations.return_value = mock_df

        result = aggregator.transform(mock_df)

        mock_tu.apply_aggregations.assert_called_once_with(
            mock_spark, mock_df, aggregator.transform_config,
        )
        assert result is mock_df

    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_transform_returns_dataframe(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify transform() returns a DataFrame."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        mock_tu.apply_aggregations.return_value = mock_df

        result = aggregator.transform(mock_df)
        assert result is not None

    # ---- load() -----------------------------------------------------------

    def test_load_append_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify load() calls write_df with append mode and gold properties."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.destination_config["mode"] = "append"

        aggregator.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "append",
            mock_delta_manager.gold_tbl_properties,
        )

    def test_load_overwrite_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify load() calls write_df with overwrite mode."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.destination_config["mode"] = "overwrite"

        aggregator.load(mock_df)

        mock_delta_manager.write_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "overwrite",
            mock_delta_manager.gold_tbl_properties,
        )

    def test_load_merge_mode(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify load() calls merge_df when mode is 'merge'."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.destination_config["mode"] = "merge"
        aggregator.destination_config["merge_condition"] = "tgt.id = src.id"

        aggregator.load(mock_df)

        mock_delta_manager.merge_df.assert_called_once_with(
            mock_df,
            "dev_catalog.schema.test_table",
            "tgt.id = src.id",
        )

    def test_load_no_target_table_raises(
        self, mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """Verify load() raises ValueError when no target table is configured."""
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        mock_env_manager.construct_table_fqn.return_value = ""

        with pytest.raises(ValueError, match="No target table configured"):
            aggregator.load(mock_df)


# ═══════════════════════════════════════════════════════════════════════════
# END-TO-END TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestGoldAggregatorE2E:
    """End-to-end tests for the full run() pipeline."""

    @patch("framework.layers.gold.gold_aggregator.Masker")
    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_run_success_full_pipeline(
        self, mock_tu, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """
        Verify the full happy-path: extract → transform → load → mask → audit.
        """
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.extract = MagicMock(return_value=mock_df)

        # Stub aggregation
        mock_tu.apply_aggregations.return_value = mock_df

        aggregator.run()

        # Verify call sequence
        aggregator.extract.assert_called_once()
        mock_tu.apply_aggregations.assert_called_once_with(
            mock_spark, mock_df, aggregator.transform_config,
        )
        mock_delta_manager.write_df.assert_called_once()
        mock_masker_cls.assert_called_once()
        mock_masker_cls.return_value.apply_masking.assert_called_once()
        mock_audit_logger.log_run_audit.assert_called_once()
        # Verify success status in audit call
        audit_args = mock_audit_logger.log_run_audit.call_args[0]
        assert "success" in audit_args
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_run_extract_returns_none_logs_and_returns(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config,
    ):
        """
        When extract() returns None, run should log a success audit
        and skip transform/load/mask.
        """
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.extract = MagicMock(return_value=None)

        aggregator.run()

        aggregator.extract.assert_called_once()
        # Audit logged as success even with no data
        mock_audit_logger.log_run_audit.assert_called_once()
        audit_args = mock_audit_logger.log_run_audit.call_args[0]
        assert "success" in audit_args
        # Transform NOT called
        mock_tu.apply_aggregations.assert_not_called()
        # Load NOT called
        mock_delta_manager.write_df.assert_not_called()
        # Finally block
        mock_audit_logger.log_run_summary.assert_called_once()
        mock_delta_handler.flush.assert_called_once()

    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_run_exception_logs_error_and_reraises(
        self, mock_tu,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """
        When an exception occurs during transform, verify error logging,
        failure audit, and re-raise.
        """
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.extract = MagicMock(return_value=mock_df)

        # Force apply_aggregations to blow up
        mock_tu.apply_aggregations.side_effect = RuntimeError("Gold aggregation error!")

        with pytest.raises(RuntimeError, match="Gold aggregation error!"):
            aggregator.run()

        aggregator.logger.error.assert_called_once()
        mock_audit_logger.log_error.assert_called_once()
        # Failure audit
        mock_audit_logger.log_run_audit.assert_called_once()
        audit_args = mock_audit_logger.log_run_audit.call_args[0]
        # First arg should be None (no df on failure)
        assert audit_args[0] is None
        # Should contain "failure"
        assert "failure" in audit_args

    @patch("framework.layers.gold.gold_aggregator.Masker")
    @patch("framework.layers.gold.gold_aggregator.TransformationUtils")
    def test_run_always_calls_finally_block(
        self, mock_tu, mock_masker_cls,
        mock_args, mock_spark, mock_env_manager,
        mock_audit_logger, mock_delta_manager, mock_delta_handler,
        gold_config, mock_df,
    ):
        """
        Verify log_run_summary() and delta_handler.flush() are called
        regardless of success or failure.
        """
        aggregator = _build_gold_aggregator(
            mock_args, mock_spark, mock_env_manager,
            mock_audit_logger, mock_delta_manager, mock_delta_handler, gold_config,
        )
        aggregator.extract = MagicMock(return_value=mock_df)

        # Stub aggregation
        mock_tu.apply_aggregations.return_value = mock_df

        # --- Success path ---
        aggregator.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1

        # Reset
        mock_audit_logger.log_run_summary.reset_mock()
        mock_delta_handler.flush.reset_mock()
        mock_audit_logger.log_run_audit.reset_mock()
        mock_audit_logger.log_error.reset_mock()

        # --- Failure path ---
        mock_tu.apply_aggregations.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            aggregator.run()
        assert mock_audit_logger.log_run_summary.call_count == 1
        assert mock_delta_handler.flush.call_count == 1
