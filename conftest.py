"""
Shared pytest fixtures for medallion layer tests.

All heavy Spark/Databricks dependencies are mocked so that tests
can run without a real Spark cluster.

Environment-aware:
  - On Databricks (PySpark available): framework imports resolve natively,
    no stubs needed.
  - On CI agents (no PySpark): sys.modules stubs make the framework
    importable without real dependencies.
"""

import sys
import types
import pytest
from unittest.mock import MagicMock


# ═══════════════════════════════════════════════════════════════════════════
# sys.modules stubs — make the framework importable everywhere
# ═══════════════════════════════════════════════════════════════════════════

def _ensure_module(name):
    """Register a stub module in sys.modules if it doesn't exist."""
    if name not in sys.modules:
        mod = types.ModuleType(name)
        sys.modules[name] = mod
    return sys.modules[name]


def _setup_framework_stubs():
    """
    Pre-populate sys.modules with stubs for every framework sub-package
    so that `from framework.layers.bronze.bronze_ingestor import BronzeIngester`
    (and similar) resolves without errors.

    Only the leaf modules that the layer files actually import need real
    attributes; everything else can be an empty module stub.
    """

    # ---- PySpark stubs ----
    pyspark = _ensure_module("pyspark")
    pyspark_sql = _ensure_module("pyspark.sql")
    pyspark_sql.SparkSession = MagicMock(name="SparkSession")
    pyspark_sql.DataFrame = MagicMock(name="DataFrame")

    pyspark_sql_functions = _ensure_module("pyspark.sql.functions")
    # Provide common function stubs
    for fn_name in [
        "col", "lit", "current_timestamp", "count", "sum", "expr",
        "concat_ws", "sha2", "lower", "upper", "trim", "regexp_replace",
        "explode", "when", "to_variant_object", "struct", "parse_json",
        "regexp_extract",
    ]:
        setattr(pyspark_sql_functions, fn_name, MagicMock(name=f"F.{fn_name}"))
    pyspark_sql_functions.F = pyspark_sql_functions  # self-reference for `import as F`

    pyspark_sql_types = _ensure_module("pyspark.sql.types")
    pyspark_sql_types.StructType = MagicMock()
    pyspark_sql_types.StructField = MagicMock()
    pyspark_sql_types.StringType = MagicMock()
    pyspark_sql_types.TimestampType = MagicMock()

    _ensure_module("pyspark.dbutils")

    # ---- delta stubs ----
    _ensure_module("delta")
    delta_tables = _ensure_module("delta.tables")
    delta_tables.DeltaTable = MagicMock(name="DeltaTable")

    # ---- databricks stubs ----
    _ensure_module("databricks")
    _ensure_module("databricks.sdk")
    databricks_sdk = sys.modules["databricks.sdk"]
    databricks_sdk.WorkspaceClient = MagicMock(name="WorkspaceClient")

    _ensure_module("databricks.labs")
    _ensure_module("databricks.labs.dqx")
    dqx_engine = _ensure_module("databricks.labs.dqx.engine")
    dqx_engine.DQEngine = MagicMock(name="DQEngine")
    dqx_engine.ExtraParams = MagicMock(name="ExtraParams")

    # ---- framework package tree ----
    _ensure_module("framework")
    _ensure_module("framework.core")
    _ensure_module("framework.core.base")
    _ensure_module("framework.core.session")
    _ensure_module("framework.core.config")
    _ensure_module("framework.core.environment")
    _ensure_module("framework.modules")
    _ensure_module("framework.modules.utils")
    _ensure_module("framework.modules.utils.transformations")
    _ensure_module("framework.modules.utils.common")
    _ensure_module("framework.modules.io")
    _ensure_module("framework.modules.io.reader")
    _ensure_module("framework.modules.io.writer")
    _ensure_module("framework.modules.audit")
    _ensure_module("framework.modules.audit.audit_logger")
    _ensure_module("framework.modules.audit.logging_setup")
    _ensure_module("framework.modules.audit.delta_log_handler")
    _ensure_module("framework.modules.security")
    _ensure_module("framework.modules.security.masker")
    _ensure_module("framework.modules.dq")
    _ensure_module("framework.modules.dq.dqx_validator")
    _ensure_module("framework.layers")
    _ensure_module("framework.layers.bronze")
    _ensure_module("framework.layers.silver")
    _ensure_module("framework.layers.gold")

    # ---- Populate framework stubs with mock classes/functions ----

    # core.base — MedallionBase
    from abc import ABC, abstractmethod

    class _StubMedallionBase(ABC):
        """Lightweight stand-in for MedallionBase so subclasses can inherit."""
        def __init__(self, args):
            pass

        def extract(self):
            pass

        @abstractmethod
        def transform(self, df):
            pass

        @abstractmethod
        def load(self, df):
            pass

        @abstractmethod
        def run(self):
            pass

    base_mod = sys.modules["framework.core.base"]
    base_mod.MedallionBase = _StubMedallionBase

    # core.session
    session_mod = sys.modules["framework.core.session"]
    session_mod.SessionManager = MagicMock(name="SessionManager")

    # core.config
    config_mod = sys.modules["framework.core.config"]
    config_mod.ConfigManager = MagicMock(name="ConfigManager")

    # core.environment
    env_mod = sys.modules["framework.core.environment"]
    env_mod.EnvironmentManager = MagicMock(name="EnvironmentManager")

    # modules.utils.transformations
    tu_mod = sys.modules["framework.modules.utils.transformations"]
    tu_mock = MagicMock(name="TransformationUtils")
    tu_mod.TransformationUtils = tu_mock

    # modules.utils.common
    common_mod = sys.modules["framework.modules.utils.common"]
    common_mod.to_bool = lambda v: bool(v)

    # modules.io.reader
    reader_mod = sys.modules["framework.modules.io.reader"]
    reader_mod.BaseFileIngester = MagicMock(name="BaseFileIngester")
    reader_mod.CSVFileIngester = MagicMock(name="CSVFileIngester")
    reader_mod.EBCDICFileIngester = MagicMock(name="EBCDICFileIngester")
    reader_mod.DeltaTableIngester = MagicMock(name="DeltaTableIngester")

    # modules.io.writer
    writer_mod = sys.modules["framework.modules.io.writer"]
    writer_mod.DeltaManager = MagicMock(name="DeltaManager")

    # modules.audit.audit_logger
    audit_mod = sys.modules["framework.modules.audit.audit_logger"]
    audit_mod.AuditLogger = MagicMock(name="AuditLogger")

    # modules.audit.logging_setup
    log_setup_mod = sys.modules["framework.modules.audit.logging_setup"]
    log_setup_mod.setup_pipeline_logging = MagicMock(name="setup_pipeline_logging")

    # modules.security.masker
    masker_mod = sys.modules["framework.modules.security.masker"]
    masker_mod.Masker = MagicMock(name="Masker")

    # modules.dq.dqx_validator
    dq_mod = sys.modules["framework.modules.dq.dqx_validator"]
    dq_mod.DataQualityCheck = MagicMock(name="DataQualityCheck")



# ═══════════════════════════════════════════════════════════════════════════
# Setup: always apply stubs + load layer modules from disk
# ═══════════════════════════════════════════════════════════════════════════
# We always use stubs even on Databricks because:
#   1. These are pure mock-based unit tests — no real PySpark needed
#   2. Databricks /Workspace/ paths have import quirks
#   3. Stubs guarantee a clean, isolated test environment

_setup_framework_stubs()

# Force-import the actual layer .py files from disk
import importlib.util
from pathlib import Path

# Dynamically resolve: conftest.py → tests/layers/ → tests/ → src/ → src/framework/layers/
_LAYER_BASE = Path(__file__).resolve().parent.parent.parent / "framework" / "layers"

def _load_layer_module(module_name, file_path):
    """Load a .py file as a module and register it in sys.modules."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

_load_layer_module(
    "framework.layers.bronze.bronze_ingestor",
    _LAYER_BASE / "bronze" / "bronze_ingestor.py",
)
_load_layer_module(
    "framework.layers.silver.silver_refiner",
    _LAYER_BASE / "silver" / "silver_refiner.py",
)
_load_layer_module(
    "framework.layers.gold.gold_aggregator",
    _LAYER_BASE / "gold" / "gold_aggregator.py",
)


# ═══════════════════════════════════════════════════════════════════════════
# Pytest Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def mock_args():
    """
    Fake argparse namespace with all attributes expected by
    MedallionBase.__init__ and its subclasses.
    """
    args = types.SimpleNamespace(
        layer="bronze",
        config_file_name="test_pipeline.yaml",
        config_directory_path="/Workspace/configs",
        databricks_job_id="12345",
        databricks_run_id="67890",
        email_id="testuser@example.com",
        correlation_id="corr-abc-123",
        test_schema=None,
        test_catalog=None,
    )
    return args


@pytest.fixture
def mock_spark():
    """
    MagicMock SparkSession with chained reader/writer/catalog/conf stubs.
    """
    spark = MagicMock(name="SparkSession")
    spark.conf.get.return_value = ""
    spark.catalog.tableExists.return_value = True

    reader = MagicMock(name="DataFrameReader")
    reader.format.return_value = reader
    reader.options.return_value = reader
    reader.load.return_value = MagicMock(name="DataFrame")
    spark.read = reader

    spark.table.return_value = MagicMock(name="TableDF")
    return spark


@pytest.fixture
def mock_df():
    """
    MagicMock DataFrame with commonly chained methods.
    """
    df = MagicMock(name="DataFrame")
    df.columns = ["col_a", "col_b", "col_c"]
    df.dtypes = [("col_a", "string"), ("col_b", "int"), ("col_c", "string")]
    df.count.return_value = 100
    df.isEmpty.return_value = False

    df.withColumn.return_value = df
    df.withColumns.return_value = df
    df.select.return_value = df
    df.filter.return_value = df
    df.where.return_value = df
    df.drop.return_value = df
    df.dropDuplicates.return_value = df
    df.fillna.return_value = df
    df.toDF.return_value = df
    df.unionByName.return_value = df

    grouped = MagicMock(name="GroupedData")
    grouped.agg.return_value = df
    df.groupBy.return_value = grouped

    df.collect.return_value = []
    return df


@pytest.fixture
def mock_env_manager():
    """MagicMock EnvironmentManager with default return values."""
    env = MagicMock(name="EnvironmentManager")
    env.env = "dev"
    env.env_prefix = "dev_"
    env.construct_table_fqn.return_value = "dev_catalog.schema.test_table"
    env.construct_source_folder_path.return_value = "/Volumes/dev_deltalake/raw/source"
    env.get_code_log_table_fqn.return_value = "dev_audit.audit_logs.code_logs"
    env.parse_cleaned_table_path.return_value = ("catalog", "schema", "test_table")
    env.get_masking_function_fqn.return_value = "dev_protect.masking.mask_ssn"
    env.get_data_quality_exceptions_fqn.return_value = "dev_audit.audit_logs.data_quality_exceptions"
    env.get_data_quality_summary_fqn.return_value = "dev_audit.audit_logs.data_quality_summary"
    env.args = MagicMock()
    return env


@pytest.fixture
def mock_audit_logger():
    """MagicMock AuditLogger with all methods used by layer classes."""
    audit = MagicMock(name="AuditLogger")
    audit._get_datetime_now.return_value = "2026-04-21T12:00:00"
    return audit


@pytest.fixture
def mock_delta_manager():
    """MagicMock DeltaManager with table property dicts."""
    dm = MagicMock(name="DeltaManager")
    dm.bronze_tbl_properties = {"delta.enableRowTracking": "true"}
    dm.silver_tbl_properties = {}
    dm.gold_tbl_properties = {}
    return dm


@pytest.fixture
def mock_delta_handler():
    """MagicMock DeltaTableHandler for the logging subsystem."""
    handler = MagicMock(name="DeltaTableHandler")
    return handler


@pytest.fixture
def bronze_config():
    """Sample bronze layer YAML config as a dict."""
    return {
        "pipeline_metadata": {
            "name": "test_bronze_pipeline",
            "description": "Bronze ingestion test",
        },
        "source": {
            "type": "file",
            "properties": {
                "source_path": "landing/test_data",
                "format": "csv",
                "options": {"header": "true", "delimiter": ","},
            },
        },
        "transform": {
            "with_columns": [
                {"name": "source_system", "expr": "'TEST_SYSTEM'"},
            ],
            "standardize": {
                "case": "upper",
                "trim_strings": True,
            },
        },
        "destination": {
            "table": "deltalake.bronze_schema.test_table",
            "mode": "append",
        },
        "security": {
            "masking": [],
        },
    }


@pytest.fixture
def silver_config():
    """Sample silver layer YAML config as a dict."""
    return {
        "pipeline_metadata": {
            "name": "test_silver_pipeline",
            "description": "Silver refinement test",
        },
        "source": {
            "type": "delta_table",
            "properties": {
                "table": "deltalake.bronze_schema.test_table",
            },
        },
        "transform": {
            "with_columns": [
                {"name": "clean_flag", "expr": "'Y'"},
            ],
            "standardize": {
                "case": "lower",
            },
            "casts": [
                {"column": "col_b", "to_type": "integer"},
            ],
        },
        "destination": {
            "table": "deltalake.silver_schema.test_table",
            "mode": "append",
        },
        "data_quality": {},
        "security": {
            "masking": [],
        },
    }


@pytest.fixture
def gold_config():
    """Sample gold layer YAML config as a dict."""
    return {
        "pipeline_metadata": {
            "name": "test_gold_pipeline",
            "description": "Gold aggregation test",
        },
        "source": {
            "type": "delta_table",
            "properties": {
                "table": "deltalake.silver_schema.test_table",
            },
        },
        "transform": {
            "aggregations": {
                "group_by": ["col_a"],
                "metrics": [
                    {"name": "total_col_b", "expr": "sum(col_b)"},
                ],
            },
        },
        "destination": {
            "table": "deltalake.gold_schema.test_table",
            "mode": "overwrite",
        },
        "security": {
            "masking": [],
        },
    }
