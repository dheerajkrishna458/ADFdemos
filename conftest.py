"""
Shared pytest fixtures for layer unit tests (Bronze, Silver, Gold).

Strategy:
  1. Snapshot sys.modules BEFORE any modifications.
  2. Stub all external + framework dependencies temporarily.
  3. Load the real layer source files (bronze, silver, gold) via _load_source.
     At load-time the classes capture their dependencies from the stubs.
  4. RESTORE sys.modules so no stubs leak into existing core tests.
  5. Re-register only the three layer modules in sys.modules.

NOTE: Tests are written against the zip-packaged source (core_utils.zip)
      which uses TransformationExecutor, DataTransformations,
      HeaderTransformations, and config keys 'pipeline'/'target'.
"""

import sys
import os
import types as _types
import importlib
import importlib.machinery
import importlib.util
import pytest
from unittest.mock import MagicMock
import abc
import logging


# ===================================================================
# 1. Add src/ to sys.path
# ===================================================================

_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


# ===================================================================
# 2. Snapshot → Stub → Load → Restore
# ===================================================================

# Take a snapshot of sys.modules BEFORE we touch anything.
# After loading the layer source files we will restore this snapshot
# so our stubs don't poison the existing core tests.
_snapshot = dict(sys.modules)


def _make_stub(name):
    """Create a minimal real module object for *name*."""
    mod = _types.ModuleType(name)
    mod.__path__ = []
    mod.__file__ = f"<stub {name}>"
    mod.__loader__ = None
    mod.__package__ = name.rsplit(".", 1)[0] if "." in name else name
    mod.__spec__ = importlib.machinery.ModuleSpec(name, None, origin=f"<stub {name}>")
    return mod


def _stub(dotted_name):
    """Ensure *dotted_name* and every parent exist in sys.modules."""
    parts = dotted_name.split(".")
    for i in range(1, len(parts) + 1):
        key = ".".join(parts[:i])
        if key not in sys.modules:
            sys.modules[key] = _make_stub(key)


# --- External packages ---
_EXTERNALS = [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.utils",
    "delta",
    "delta.tables",
    "databricks",
    "databricks.sdk",
    "databricks.labs",
    "databricks.labs.dqx",
    "databricks.labs.dqx.engine",
]

for _m in _EXTERNALS:
    _stub(_m)

sys.modules["pyspark.sql"].SparkSession = MagicMock(name="SparkSession")
sys.modules["pyspark.sql"].DataFrame = MagicMock(name="DataFrame")
_F = MagicMock(name="pyspark.sql.functions")
sys.modules["pyspark.sql"].functions = _F
sys.modules["pyspark.sql.functions"] = _F
_T = MagicMock(name="pyspark.sql.types")
sys.modules["pyspark.sql"].types = _T
sys.modules["pyspark.sql.types"] = _T
sys.modules["delta.tables"].DeltaTable = MagicMock(name="DeltaTable")
sys.modules["databricks.sdk"].WorkspaceClient = MagicMock(name="WorkspaceClient")
sys.modules["databricks.labs.dqx.engine"].DQEngine = MagicMock(name="DQEngine")
sys.modules["databricks.labs.dqx.engine"].ExtraParams = MagicMock(name="ExtraParams")


# --- Framework modules ---
_FRAMEWORK_STUBS = [
    "framework",
    "framework.core",
    "framework.core.base",
    "framework.core.session",
    "framework.core.config",
    "framework.core.environment",
    "framework.core.enum",
    "framework.core.enums",
    "framework.core.arg_parser",
    "framework.core.plan",
    "framework.core.plan_printer",
    "framework.core.runtime",
    "framework.modules",
    "framework.modules.audit",
    "framework.modules.audit.audit_logger",
    "framework.modules.audit.yaml_telemetry_logger",
    "framework.modules.audit.delta_log_handler",
    "framework.modules.logging",
    "framework.modules.logging.log_manager",
    "framework.modules.io",
    "framework.modules.io.reader",
    "framework.modules.io.writer",
    "framework.modules.dq",
    "framework.modules.dq.dqx_validator",
    "framework.modules.security",
    "framework.modules.security.masker",
    "framework.modules.security.encryption",
    "framework.modules.security.tokenizer",
    "framework.modules.transformations",
    "framework.modules.transformations.executor",
    "framework.modules.transformations.data",
    "framework.modules.transformations.headers",
    "framework.modules.utils",
    "framework.modules.utils.common",
    "framework.modules.utils.handle_dry_run",
    "framework.modules.utils.plan",
    "framework.layers",
    "framework.layers.bronze",
    "framework.layers.silver",
    "framework.layers.gold",
]

for _m in _FRAMEWORK_STUBS:
    _stub(_m)

sys.modules["framework.core.session"].SessionManager = MagicMock(name="SessionManager")
sys.modules["framework.core.config"].ConfigManager = MagicMock(name="ConfigManager")
sys.modules["framework.core.environment"].EnvironmentManager = MagicMock(name="EnvironmentManager")
sys.modules["framework.modules.audit.audit_logger"].AuditLogger = MagicMock(name="AuditLogger")
sys.modules["framework.modules.audit.yaml_telemetry_logger"].YamlTelemetryLogger = MagicMock(name="YamlTelemetryLogger")
sys.modules["framework.modules.logging.log_manager"].LoggingConfigurator = MagicMock(name="LoggingConfigurator")
sys.modules["framework.modules.io.reader"].BaseFileIngester = MagicMock(name="BaseFileIngester")
sys.modules["framework.modules.io.reader"].CSVFileIngester = MagicMock(name="CSVFileIngester")
sys.modules["framework.modules.io.reader"].EBCDICFileIngester = MagicMock(name="EBCDICFileIngester")
sys.modules["framework.modules.io.reader"].DeltaTableIngester = MagicMock(name="DeltaTableIngester")
sys.modules["framework.modules.io.writer"].DeltaManager = MagicMock(name="DeltaManager")
sys.modules["framework.modules.dq.dqx_validator"].DataQualityCheck = MagicMock(name="DataQualityCheck")
sys.modules["framework.modules.security.masker"].Masker = MagicMock(name="Masker")
sys.modules["framework.modules.utils.common"].to_bool = MagicMock(return_value=True)

sys.modules["framework.modules.transformations.executor"].TransformationExecutor = MagicMock(name="TransformationExecutor")
sys.modules["framework.modules.transformations.data"].DataTransformations = MagicMock(name="DataTransformations")
sys.modules["framework.modules.transformations.headers"].HeaderTransformations = MagicMock(name="HeaderTransformations")

_mock_load_status = MagicMock(name="LoadStatus")
_mock_load_status.SUCCESS.value = "Success"
_mock_load_status.FAILURE.value = "Failed"
_mock_operation_type = MagicMock(name="OperationType")
_mock_operation_type.APPEND.value = "APPEND"
_mock_operation_type.from_mode.return_value.value = "APPEND"
_mock_audit_columns = MagicMock(name="AuditColumns")
_mock_audit_columns.values.return_value = ["databricks_job_id", "databricks_run_id", "correlation_id"]

sys.modules["framework.core.enum"].LoadStatus = _mock_load_status
sys.modules["framework.core.enum"].OperationType = _mock_operation_type
sys.modules["framework.core.enum"].AuditColumns = _mock_audit_columns


# ===================================================================
# 3. Fake MedallionBase
# ===================================================================

class _FakeMedallionBase(abc.ABC):
    """Drop-in replacement for ``framework.core.base.MedallionBase``."""

    def __init__(self, args):
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )
        self.args = args
        self.spark = MagicMock(name="SparkSession")
        self.env_manager = MagicMock(name="EnvironmentManager")
        self.audit_logger = MagicMock(name="AuditLogger")
        self.delta_manager = MagicMock(name="DeltaManager")
        self.delta_handler = MagicMock(name="DeltaHandler")
        self.config = {}
        self.source_config = {}
        self.files_discovered = []

    def extract(self):
        pass

    @abc.abstractmethod
    def transform(self, df):
        pass

    @abc.abstractmethod
    def load(self, df):
        pass

    @abc.abstractmethod
    def run(self):
        pass


sys.modules["framework.core.base"].MedallionBase = _FakeMedallionBase


# ===================================================================
# 4. Load the real layer source files
# ===================================================================

for _key in list(sys.modules.keys()):
    if "bronze_ingester" in _key or "silver_refiner" in _key or "gold_aggregator" in _key:
        del sys.modules[_key]


def _load_source(module_name, file_path):
    """Load a Python source file as a module, respecting our sys.modules stubs."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_LAYERS_DIR = os.path.join(_SRC_DIR, "framework", "layers")

_bronze_mod = _load_source(
    "framework.layers.bronze.bronze_ingester",
    os.path.join(_LAYERS_DIR, "bronze", "bronze_ingester.py"),
)

_silver_mod = _load_source(
    "framework.layers.silver.silver_refiner",
    os.path.join(_LAYERS_DIR, "silver", "silver_refiner.py"),
)

_gold_mod = _load_source(
    "framework.layers.gold.gold_aggregator",
    os.path.join(_LAYERS_DIR, "gold", "gold_aggregator.py"),
)

BronzeIngester = _bronze_mod.BronzeIngester
SilverRefiner = _silver_mod.SilverRefiner
GoldAggregator = _gold_mod.GoldAggregator


# ===================================================================
# 5. RESTORE sys.modules — remove stubs so core tests are not poisoned
# ===================================================================
# The layer classes already captured all their dependencies (from stubs)
# in their module __dict__ via ``from ... import ...``.  Removing the
# stubs from sys.modules does NOT affect those already-bound references.

# Layer module keys to preserve
_LAYER_KEYS = {
    "framework.layers.bronze.bronze_ingester",
    "framework.layers.silver.silver_refiner",
    "framework.layers.gold.gold_aggregator",
}

# Remove everything we added that wasn't in the original snapshot
for _key in list(sys.modules.keys()):
    if _key in _LAYER_KEYS:
        continue  # keep our loaded layer modules
    if _key not in _snapshot:
        del sys.modules[_key]

# Restore any modules we overwrote
for _key, _mod in _snapshot.items():
    if _key in sys.modules and sys.modules[_key] is not _mod:
        sys.modules[_key] = _mod


# ===================================================================
# 6. Fixtures
# ===================================================================

@pytest.fixture
def bronze_module():
    """Return the loaded bronze_ingester module object for patch.object()."""
    return _bronze_mod


@pytest.fixture
def silver_module():
    """Return the loaded silver_refiner module object for patch.object()."""
    return _silver_mod


@pytest.fixture
def gold_module():
    """Return the loaded gold_aggregator module object for patch.object()."""
    return _gold_mod


@pytest.fixture
def mock_args():
    """Return a namespace-like object with all CLI args the layers need."""
    return _types.SimpleNamespace(
        layer="bronze",
        config_file_name="test_pipeline.yml",
        env="dev",
        databricks_job_id="1001",
        databricks_run_id="2002",
        email_id="test@example.com",
        correlation_id="corr-1234",
        test_catalog=None,
        test_schema=None,
        dry_run=False,
        entry_point="pytest",
        manual_intervention_reason=None,
    )


@pytest.fixture
def bronze_config():
    return {
        "pipeline": {"name": "test_bronze_pipeline"},
        "source": {
            "type": "file",
            "properties": {
                "source_path": "raw/bronze_test",
                "format": "csv",
                "options": {"header": "true"},
            },
        },
        "transform": {
            "standardize_column_names": {"enabled": True},
        },
        "target": {
            "table": "catalog.schema.bronze_tbl",
            "mode": "append",
        },
        "security": {"masking": []},
    }


@pytest.fixture
def silver_config():
    return {
        "pipeline": {"name": "test_silver_pipeline"},
        "source": {
            "type": "delta_table",
            "properties": {"table": "catalog.schema.bronze_tbl"},
        },
        "transform": {
            "standardize_column_names": {"enabled": True},
            "casts": [{"column": "amount", "to_type": "double"}],
        },
        "target": {
            "table": "catalog.schema.silver_tbl",
            "mode": "overwrite",
        },
        "data_quality": {},
        "security": {"masking": []},
    }


@pytest.fixture
def gold_config():
    return {
        "pipeline": {"name": "test_gold_pipeline"},
        "source": {
            "type": "delta_table",
            "properties": {"table": "catalog.schema.silver_tbl"},
        },
        "transform": {
            "standardize_column_names": {"enabled": True},
            "aggregations": {
                "group_by": ["region"],
                "metrics": [{"name": "total_sales", "expr": "sum(sales)"}],
            },
        },
        "target": {
            "table": "catalog.schema.gold_tbl",
            "mode": "append",
        },
        "security": {"masking": []},
    }


@pytest.fixture
def mock_spark():
    spark = MagicMock(name="SparkSession")
    spark.conf.get.return_value = ""
    spark.catalog.tableExists.return_value = True
    return spark


@pytest.fixture
def mock_env_manager():
    em = MagicMock(name="EnvironmentManager")
    em.env = "dev"
    em.env_prefix = "dev_"
    em.construct_table_fqn.side_effect = lambda t: f"dev_{t}" if t else None
    em.construct_source_folder_path.side_effect = (
        lambda p: f"/Volumes/dev_deltalake/raw/{p.lstrip('/')}"
    )
    em.get_code_log_table_fqn.return_value = "dev_deltalake_audit.audit_logs.code_logs"
    em.parse_cleaned_table_path.return_value = ("catalog", "schema", "table")
    return em


@pytest.fixture
def mock_audit_logger():
    al = MagicMock(name="AuditLogger")
    al._get_datetime_now.return_value = "2026-01-01T00:00:00"
    return al


@pytest.fixture
def mock_delta_manager():
    dm = MagicMock(name="DeltaManager")
    dm.bronze_tbl_properties = {"delta.enableRowTracking": "true"}
    dm.silver_tbl_properties = {}
    dm.gold_tbl_properties = {}
    return dm


@pytest.fixture
def mock_delta_handler():
    return MagicMock(name="DeltaHandler")


@pytest.fixture
def mock_dataframe():
    df = MagicMock(name="DataFrame")
    df.columns = ["col_a", "col_b", "file_name"]
    df.count.return_value = 10
    df.isEmpty.return_value = False
    return df


@pytest.fixture
def patch_base_init(
    mock_spark, mock_env_manager, mock_audit_logger,
    mock_delta_manager, mock_delta_handler,
):
    """
    Patch ``_FakeMedallionBase.__init__`` so layer constructors get our
    pre-configured mock objects instead of creating their own.
    """
    mocks = {
        "spark": mock_spark,
        "env_manager": mock_env_manager,
        "audit_logger": mock_audit_logger,
        "delta_manager": mock_delta_manager,
        "delta_handler": mock_delta_handler,
    }

    original_init = _FakeMedallionBase.__init__

    def fake_init(self, args):
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )
        self.args = args
        self.spark = mocks["spark"]
        self.env_manager = mocks["env_manager"]
        self.audit_logger = mocks["audit_logger"]
        self.delta_manager = mocks["delta_manager"]
        self.delta_handler = mocks["delta_handler"]
        self.config = {}
        self.source_config = {}
        self.files_discovered = []

    _FakeMedallionBase.__init__ = fake_init
    yield mocks
    _FakeMedallionBase.__init__ = original_init
