# tests/test_multi_split_csv_integration.py

import pytest
from unittest.mock import patch, Mock, call, ANY
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, TimestampType,
    LongType, MapType
)
from datetime import datetime as dt

# -----------------------------
# Helper functions (once per module)
# -----------------------------
def _normalize_cell(v):
    # Make rows comparable: convert dict/map to sorted tuple, keep primitives as-is.
    if isinstance(v, dict):
        return tuple(sorted(v.items()))
    return v

def _normalize_row(row_tuple):
    return tuple(_normalize_cell(v) for v in row_tuple)

def assert_dataframe_equal_unordered(actual_df: DataFrame, expected_df: DataFrame, cols=None):
    """
    Compare two DataFrames by (subset) schema and unordered content.
    If cols is provided, compare only those columns; otherwise use expected_df.columns.
    Allows actual_df to have extra columns not present in expected_df.
    """
    # --- schema subset check
    actual_fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in actual_df.schema.fields]
    expected_fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in expected_df.schema.fields]
    assert set(expected_fields).issubset(set(actual_fields)), \
        f"\nExpected schema subset:\n{expected_fields}\nActual schema:\n{actual_fields}"

    # --- select columns to compare
    if cols is None:
        cols = expected_df.columns
    missing = set(cols) - set(actual_df.columns)
    assert not missing, f"Actual DF missing expected columns: {missing}"

    exp_rows = [_normalize_row(tuple(r[c] for c in cols)) for r in expected_df.select(*cols).collect()]
    act_rows = [_normalize_row(tuple(r[c] for c in cols)) for r in actual_df.select(*cols).collect()]
    assert sorted(act_rows) == sorted(exp_rows), \
        f"\nExpected rows (subset {cols}):\n{sorted(exp_rows)}\nActual rows:\n{sorted(act_rows)}"


# -----------------------------
# The test (no mocking of _get_paths_with_and_without_header)
# -----------------------------
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.ConfigManager.update_config_table_schema')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.AuditLogger._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_current_timestamp')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_datetime_now')
# NOTE: Intentionally NOT patching BronzeWriter._get_paths_with_and_without_header
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_current_timestamp')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.append_df_to_table')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.merge_df_to_table')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.datetime')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._adls_detect_encoding')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_new_paths_list')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.set_table_properties')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.WidgetManager')
def test_multi_split_csv_success(
    widgets_mock, set_properties_mock, new_paths_list_mock, adls_detect_encoding_mock, datetime_mock,
    merge_mock, append_mock, file_list_current_timestamp_mock, file_list_time_mock,
    bronze_writer_time_mock, bronze_writer_current_timestamp_mock, audit_logger_now_mock,
    config_schema_update_mock, spark_fixture
):
    # -------------------------
    # Widgets
    # -------------------------
    def return_widgets_value(widget_name):
        match widget_name:
            case 'email_id': return 'TR_ExampleTrigger'
            case 'databricks_run_id': return '8507742291424483'
            case 'databricks_job_id': return '111222333'
            case 'adf_run_id': return 'corr_id_123'
            case _: return None

    widgets_mock.return_value = widgets_mock
    widgets_mock.get_value.side_effect = return_widgets_value

    widgets_mock.category_1_parameter = None
    widgets_mock.category_2_parameter = None
    widgets_mock.email_id = 'TR_ExampleTrigger'
    widgets_mock.databricks_run_id = '8507742291424483'
    widgets_mock.databricks_job_id = '111222333'
    widgets_mock.adf_run_id = 'corr_id_123'
    widgets_mock.begin_date = None
    widgets_mock.end_date = None
    widgets_mock.target_table_name = None
    widgets_mock.manual_intervention_reason = None
    widgets_mock.replacement_file_name = None

    # -------------------------
    # File list (state machine)
    # -------------------------
    # ADJUST IF NEEDED:
    from file_manager import FileManager
    file_list_df = Mock()
    file_list_schema = FileManager(spark_fixture, None, None, Mock(), None, None)._file_list_schema()

    file_list_values = [
        spark_fixture.createDataFrame([
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv', 'FakeFile_20250129_1.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 0, dt(2025,4,2,12,0,0), None),
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv', 'FakeFile_20250129_2.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 0, dt(2025,4,2,12,0,0), None),
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv', 'FakeFile_20250129_3.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 0, dt(2025,4,2,12,0,0), None),
        ], file_list_schema),
        spark_fixture.createDataFrame([
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv', 'FakeFile_20250129_1.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 1, dt(2025,4,2,12,0,0), None),
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv', 'FakeFile_20250129_2.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 1, dt(2025,4,2,12,0,0), None),
            ('/Fiserv/FakeFile/', 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv', 'FakeFile_20250129_3.csv', dt(2025,2,2,14,0,0), 5000, 'csv','utf-8','111222333','8507742291424483', 1, dt(2025,4,2,12,0,0), None),
        ], file_list_schema),
    ]
    file_list_df.side_effect = file_list_values

    # -------------------------
    # Config table (proper types)
    # -------------------------
    config_file_df = spark_fixture.createDataFrame(
        [('FiServ','Demographics','demographic','bronze_fiserv','fiserv_FakeFile','/Fiserv/FakeFile/','UTF-16', True, 'csv','csv','|', True, 0.02, dt(2025,1,1,12,0,0))],
        ('category_1','category_2','catalog_name','schema_name','target_table_name','source_folder','file_encoding_type','has_header','file_type','file_extension','column_delimiter','is_active','allowed_threshold','updated_timestamp'),
        StructType([
            StructField('category_1', StringType(), True),
            StructField('category_2', StringType(), True),
            StructField('catalog_name', StringType(), True),
            StructField('schema_name', StringType(), True),
            StructField('target_table_name', StringType(), True),
            StructField('source_folder', StringType(), True),
            StructField('file_encoding_type', StringType(), True),
            StructField('has_header', BooleanType(), True),
            StructField('file_type', StringType(), True),
            StructField('file_extension', StringType(), True),
            StructField('column_delimiter', StringType(), True),
            StructField('is_active', BooleanType(), True),
            StructField('allowed_threshold', DoubleType(), True),
            StructField('updated_timestamp', TimestampType(), True),
        ])
    )
    config_table_df = spark_fixture.createDataFrame(
        [('FiServ','Demographics','demographic','bronze_fiserv','fiserv_FakeFile','/Fiserv/FakeFile/','UTF-16', True, 'csv','csv','|', True, 0.02, dt(2025,1,1,12,0,0))],
        config_file_df.schema.fieldNames(),
        config_file_df.schema
    )

    def table_return_value(path):
        match path:
            case 'test_catalog.example_schema.config_table':
                return config_table_df
            case 'test_catalog.example_schema.file_list':
                return file_list_df()

    # -------------------------
    # CSV reads (header list + no-header list)
    # -------------------------
    header_path = 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv'
    no_header_paths = [
        'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv',
        'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv'
    ]

    def csv_return_value(path):
        if path == '/Volumes/dev_deltalake/raw/config-files/bronze_config_table.csv':
            return config_file_df

        # Header load: list with single header path
        if isinstance(path, list) and path == [header_path]:
            hdr_schema = StructType([
                StructField('col_1', StringType(), True),
                StructField('col_2', StringType(), True)
            ])
            return spark_fixture.createDataFrame([('1','2')], schema=hdr_schema)

        # No-header load: list with both no-header paths at once (uses header schema)
        if isinstance(path, list) and path == no_header_paths:
            hdr_schema = StructType([
                StructField('col_1', StringType(), True),
                StructField('col_2', StringType(), True)
            ])
            # rows from _2 and _3 combined
            return spark_fixture.createDataFrame([('3','4'), ('5','6'), ('7','8')], schema=hdr_schema)

        # Fallback (shouldn't be used in the multi-split path)
        if isinstance(path, str) and path in no_header_paths:
            hdr_schema = StructType([
                StructField('col_1', StringType(), True),
                StructField('col_2', StringType(), True)
            ])
            return spark_fixture.createDataFrame([('X','Y')], schema=hdr_schema)

        assert False, f"Unexpected csv path: {path}"

    spark_mock = Mock()
    spark_mock.table.side_effect = table_return_value
    spark_mock.read.option.return_value = spark_mock.read
    spark_mock.read.options.return_value = spark_mock.read
    spark_mock.read.csv.side_effect = csv_return_value
    spark_mock.createDataFrame = spark_fixture.createDataFrame

    # -------------------------
    # Mocks for times/encoding/append return
    # -------------------------
    append_mock.return_value = {
        'FakeFile_20250129_1.csv': 1,
        'FakeFile_20250129_2.csv': 3,
        'FakeFile_20250129_3.csv': 3
    }
    adls_detect_encoding_mock.return_value = 'utf-8'

    datetime_mock.now.return_value = dt(2025,2,2,12,0,0)
    datetime_mock.combine.return_value = dt(2025,3,2,12,0,0)
    file_list_time_mock.return_value = dt(2025,4,2,12,0,0)
    bronze_writer_time_mock.return_value = dt(2025,4,2,12,30,0)
    audit_logger_now_mock.return_value = dt(2025,4,2,12,55,0)
    file_list_current_timestamp_mock.return_value = F.lit(dt(2025,4,2,5,0,0)).cast('timestamp')
    bronze_writer_current_timestamp_mock.return_value = F.lit(dt(2025,4,2,5,45,0)).cast('timestamp')

    # -------------------------
    # ADLS discovery (“new paths”)
    # -------------------------
    mock_file_1 = Mock()
    mock_file_1.path = header_path
    mock_file_1.name = "FakeFile_20250129_1.csv"
    mock_file_1.modificationTime = dt(2025,2,2,14,0,0)
    mock_file_1.size = 5000

    mock_file_2 = Mock()
    mock_file_2.path = no_header_paths[0]
    mock_file_2.name = "FakeFile_20250129_2.csv"
    mock_file_2.modificationTime = dt(2025,2,2,14,0,0)
    mock_file_2.size = 5000

    mock_file_3 = Mock()
    mock_file_3.path = no_header_paths[1]
    mock_file_3.name = "FakeFile_20250129_3.csv"
    mock_file_3.modificationTime = dt(2025,2,2,14,0,0)
    mock_file_3.size = 5000

    new_paths_list_mock.return_value = [mock_file_1, mock_file_2, mock_file_3]

    # -------------------------
    # Execute ETL
    # -------------------------
    # ADJUST IF NEEDED:
    from BRONZE_ETL import BronzeETLJob
    dbutils_input_mock = Mock()
    BronzeETLJob(spark_mock, dbutils_input_mock, 'test_catalog', 'example_schema').run()

    # -------------------------
    # Build expected DFs for assertions
    # -------------------------
    # 1) Merge expectations (source DFs)
    file_list_1_df = spark_fixture.createDataFrame([{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': header_path, 'file_name':'FakeFile_20250129_1.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0), 'updated_timestamp':None
    },{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': no_header_paths[0], 'file_name':'FakeFile_20250129_2.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0), 'updated_timestamp':None
    },{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': no_header_paths[1], 'file_name':'FakeFile_20250129_3.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0), 'updated_timestamp':None
    }], file_list_schema)

    file_list_2_df = spark_fixture.createDataFrame([{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': header_path, 'file_name':'FakeFile_20250129_1.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0),
        'new_run_id':'8507742291424483', 'new_status':1, 'new_timestamp':dt(2025,4,2,5,0,0),
        'updated_timestamp':None
    },{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': no_header_paths[0], 'file_name':'FakeFile_20250129_2.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0),
        'new_run_id':'8507742291424483', 'new_status':1, 'new_timestamp':dt(2025,4,2,5,0,0),
        'updated_timestamp':None
    },{
        'source_folder':'/Fiserv/FakeFile/',
        'file_path': no_header_paths[1], 'file_name':'FakeFile_20250129_3.csv',
        'file_modification_time': dt(2025,2,2,14,0,0), 'file_size_in_bytes':5000,
        'file_extension':'csv', 'expected_encoding_type':'utf-8',
        'databricks_job_id':'111222333', 'databricks_run_id':'8507742291424483',
        'loaded_to_bronze':0, 'created_timestamp':dt(2025,4,2,12,0,0),
        'new_run_id':'8507742291424483', 'new_status':1, 'new_timestamp':dt(2025,4,2,5,0,0),
        'updated_timestamp':None
    }], StructType([
        StructField('created_timestamp', TimestampType(), True),
        StructField('databricks_job_id', StringType(), True),
        StructField('databricks_run_id', StringType(), True),
        StructField('expected_encoding_type', StringType(), True),
        StructField('file_extension', StringType(), True),
        StructField('file_modification_time', TimestampType(), True),
        StructField('file_name', StringType(), True),
        StructField('file_path', StringType(), True),
        StructField('file_size_in_bytes', LongType(), True),
        StructField('loaded_to_bronze', LongType(), True),
        StructField('new_run_id', StringType(), True),
        StructField('new_status', LongType(), True),
        StructField('new_timestamp', TimestampType(), True),
        StructField('source_folder', StringType(), True),
        StructField('updated_timestamp', TimestampType(), True),
    ]))

    file_list_3_df = spark_fixture.createDataFrame([{
        'file_path': header_path, 'databricks_run_id':'8507742291424483',
        'new_status':2, 'new_timestamp':dt(2025,4,2,12,30,0)
    },{
        'file_path': no_header_paths[0], 'databricks_run_id':'8507742291424483',
        'new_status':2, 'new_timestamp':dt(2025,4,2,12,30,0)
    },{
        'file_path': no_header_paths[1], 'databricks_run_id':'8507742291424483',
        'new_status':2, 'new_timestamp':dt(2025,4,2,12,30,0)
    }], StructType([
        StructField('databricks_run_id', StringType(), True),
        StructField('file_path', StringType(), True),
        StructField('new_status', LongType(), True),
        StructField('new_timestamp', TimestampType(), True),
    ]))

    # 2) Append expectations: three file writes + run_load_history
    expected_written_df_1 = spark_fixture.createDataFrame([{
        'file_name':'FakeFile_20250129_1.csv',
        'col_1':'1','col_2':'2',
        '_metadata':{'file_name':'FakeFile_20250129.csv'},
        'load_create_user_id':'TR_ExampleTrigger','databricks_job_id':'111222333',
        'correlation_id':'corr_id_123','databricks_run_id':'8507742291424483',
        'load_create_dtm':dt(2025,4,2,5,45,0),
        'load_update_user_id':None,'load_update_dtm':None
    }], StructType([
        StructField('_metadata', MapType(StringType(), StringType(), True), True),
        StructField('col_1', StringType(), True),
        StructField('col_2', StringType(), True),
        StructField('correlation_id', StringType(), True),
        StructField('databricks_job_id', StringType(), True),
        StructField('databricks_run_id', StringType(), True),
        StructField('file_name', StringType(), True),
        StructField('load_create_dtm', TimestampType(), True),
        StructField('load_create_user_id', StringType(), True),
        StructField('load_update_user_id', StringType(), True),
        StructField('load_update_dtm', TimestampType(), True),
    ]))

    expected_written_df_2 = spark_fixture.createDataFrame([{
        'file_name':'FakeFile_20250129_2.csv',
        'col_1':'3','col_2':'4',
        '_metadata':{'file_name':'FakeFile_20250129.csv'},
        'load_create_user_id':'TR_ExampleTrigger','databricks_job_id':'111222333',
        'correlation_id':'corr_id_123','databricks_run_id':'8507742291424483',
        'load_create_dtm':dt(2025,4,2,5,45,0),
        'load_update_user_id':None,'load_update_dtm':None
    }], expected_written_df_1.schema)

    expected_written_df_3 = spark_fixture.createDataFrame([{
        'file_name':'FakeFile_20250129_3.csv',
        'col_1':'7','col_2':'8',
        '_metadata':{'file_name':'FakeFile_20250129.csv'},
        'load_create_user_id':'TR_ExampleTrigger','databricks_job_id':'111222333',
        'correlation_id':'corr_id_123','databricks_run_id':'8507742291424483',
        'load_create_dtm':dt(2025,4,2,5,45,0),
        'load_update_user_id':None,'load_update_dtm':None
    }], expected_written_df_1.schema)

    expected_run_load_history_df = spark_fixture.createDataFrame([{
        'databricks_job_id':'111222333','databricks_run_id':'8507742291424483','correlation_id':'corr_id_123',
        'source_name':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129.csv',
        'target_name':'test_catalog.example_schema.fiserv_FakeFile',
        'load_stage_name':'RawToBronze','operation_type':'Append',
        'records_total':3,'records_succeeded':3,'records_rejected':0,
        'records_deleted':0,'records_duplicate':0,'load_status':'Success',
        'load_type_name':'Pipeline','load_create_user_id':'TR_ExampleTrigger',
        'end_dtm':dt(2025,4,2,12,55,0),'start_dtm':dt(2025,4,2,12,30,0),
        'manual_intervention_reason':None
    }], StructType([
        StructField('correlation_id', StringType(), True),
        StructField('databricks_job_id', StringType(), True),
        StructField('databricks_run_id', StringType(), True),
        StructField('end_dtm', TimestampType(), True),
        StructField('load_create_user_id', StringType(), True),
        StructField('load_stage_name', StringType(), True),
        StructField('load_status', StringType(), True),
        StructField('load_type_name', StringType(), True),
        StructField('operation_type', StringType(), True),
        StructField('records_deleted', LongType(), True),
        StructField('records_duplicate', LongType(), True),
        StructField('records_rejected', LongType(), True),
        StructField('records_succeeded', LongType(), True),
        StructField('records_total', LongType(), True),
        StructField('source_name', StringType(), True),
        StructField('start_dtm', TimestampType(), True),
        StructField('target_name', StringType(), True),
        StructField('manual_intervention_reason', StringType(), True),
    ]))

    # -------------------------
    # Expected merge calls (content + params)
    # -------------------------
    expected_merge_calls = [
        # Merge 0: update config table from config file
        {
            'source_df': config_file_df,
            'target_table': 'test_catalog.example_schema.config_table',
            'merge_condition': 'src.source_folder = tgt.source_folder',
            'update_dict': {
                'category_1': 'src.category_1', 'category_2': 'src.category_2',
                'catalog_name': 'src.catalog_name', 'schema_name': 'src.schema_name',
                'target_table_name': 'src.target_table_name', 'source_folder': 'src.source_folder',
                'file_encoding_type': 'src.file_encoding_type', 'has_header': 'src.has_header',
                'file_type': 'src.file_type', 'file_extension': 'src.file_extension',
                'column_delimiter': 'src.column_delimiter', 'is_active': 'src.is_active',
                'allowed_threshold': 'src.allowed_threshold', 'updated_timestamp': 'src.updated_timestamp'
            },
            'insert_dict': ANY,
            'update_condition': 'src.updated_timestamp > tgt.updated_timestamp'
        },
        # Merge 1: initial file_list upsert (PENDING)
        {
            'source_df': file_list_1_df,
            'target_table': 'test_catalog.example_schema.file_list',
            'merge_condition': 'tgt.file_name = src.file_name',
            'update_dict': {
                'source_folder': 'src.source_folder', 'file_path': 'src.file_path', 'file_name': 'src.file_name',
                'file_modification_time': 'src.file_modification_time', 'file_size_in_bytes': 'src.file_size_in_bytes',
                'file_extension': 'src.file_extension', 'expected_encoding_type': 'src.expected_encoding_type',
                'databricks_job_id': 'src.databricks_job_id', 'databricks_run_id': 'src.databricks_run_id',
                'loaded_to_bronze': 'src.loaded_to_bronze', 'created_timestamp': 'src.created_timestamp',
                'updated_timestamp': 'src.updated_timestamp'
            },
            'insert_dict': {
                'source_folder': 'src.source_folder', 'file_path': 'src.file_path', 'file_name': 'src.file_name',
                'file_modification_time': 'src.file_modification_time', 'file_size_in_bytes': 'src.file_size_in_bytes',
                'file_extension': 'src.file_extension', 'expected_encoding_type': 'src.expected_encoding_type',
                'databricks_job_id': 'src.databricks_job_id', 'databricks_run_id': 'src.databricks_run_id',
                'loaded_to_bronze': 'src.loaded_to_bronze', 'created_timestamp': 'src.created_timestamp',
                'updated_timestamp': 'src.updated_timestamp'
            }
        },
        # Merge 2: set PROCESSING for current run
        {
            'source_df': file_list_2_df,
            'target_table': 'test_catalog.example_schema.file_list',
            'merge_condition': 'tgt.file_path = src.file_path',
            'update_dict': {
                'databricks_run_id': 'src.new_run_id',
                'loaded_to_bronze': 'src.new_status',
                'updated_timestamp': 'src.new_timestamp'
            },
            'update_condition': 'tgt.loaded_to_bronze = 0'
        },
        # Merge 3: set SUCCESS for current run (run_id match)
        {
            'source_df': file_list_3_df,
            'target_table': 'test_catalog.example_schema.file_list',
            'merge_condition': 'tgt.file_path = src.file_path',
            'update_dict': {
                'loaded_to_bronze': 'src.new_status',
                'updated_timestamp': 'src.new_timestamp'
            },
            'update_condition': 'tgt.loaded_to_bronze = 1 AND tgt.databricks_run_id = src.databricks_run_id'
        }
    ]

    # -------------------------
    # Assert MERGE calls (order + key kwargs + DF contents)
    # -------------------------
    merge_calls = merge_mock.call_args_list
    assert len(merge_calls) >= len(expected_merge_calls), \
        f"merge_df_to_table called {len(merge_calls)} times, expected >= {len(expected_merge_calls)}"

    for i, expected in enumerate(expected_merge_calls):
        _, kwargs = merge_calls[i]

        # target table and conditions
        assert kwargs['target_table'] == expected['target_table'], f"Merge[{i}] target_table mismatch"
        assert kwargs['merge_condition'] == expected['merge_condition'], f"Merge[{i}] merge_condition mismatch"
        if 'update_condition' in expected:
            assert kwargs.get('update_condition') == expected['update_condition'], f"Merge[{i}] update_condition mismatch"

        # dict keys (avoid strict equality on values that are SQL expr strings – just check keys)
        assert set(kwargs['update_dict'].keys()) == set(expected['update_dict'].keys()), f"Merge[{i}] update_dict keys mismatch"
        if expected.get('insert_dict', None) is not None and expected['insert_dict'] is not ANY:
            assert set(kwargs['insert_dict'].keys()) == set(expected['insert_dict'].keys()), f"Merge[{i}] insert_dict keys mismatch"

        # DF contents (subset of columns present on both)
        exp_src_df = expected['source_df']
        act_src_df = kwargs['source_df']
        common_cols = [c for c in exp_src_df.columns if c in act_src_df.columns]
        assert_dataframe_equal_unordered(act_src_df, exp_src_df, cols=common_cols)

    # -------------------------
    # Assert APPEND calls (content + table + join keys)
    # -------------------------
    expected_append = [
        {
            "df": expected_written_df_1,
            "table": "test_catalog.example_schema.fiserv_FakeFile",
            "join_on": ["file_name"],
            "cols": ["file_name", "col_1", "col_2", "correlation_id", "databricks_job_id", "databricks_run_id", "load_create_dtm", "load_create_user_id"]
        },
        {
            "df": expected_written_df_2,
            "table": "test_catalog.example_schema.fiserv_FakeFile",
            "join_on": ["file_name"],
            "cols": ["file_name", "col_1", "col_2", "correlation_id", "databricks_job_id", "databricks_run_id", "load_create_dtm", "load_create_user_id"]
        },
        {
            "df": expected_written_df_3,
            "table": "test_catalog.example_schema.fiserv_FakeFile",
            "join_on": ["file_name"],
            "cols": ["file_name", "col_1", "col_2", "correlation_id", "databricks_job_id", "databricks_run_id", "load_create_dtm", "load_create_user_id"]
        },
        {
            "df": expected_run_load_history_df,
            "table": "test_catalog.example_schema.run_load_history",
            # adjust to [] or None if your implementation doesn't pass join keys for history
            "join_on": ["file_name"],
            "cols": ["databricks_job_id","databricks_run_id","correlation_id","source_name","target_name","load_stage_name","operation_type","records_total","records_succeeded","records_rejected","records_deleted","records_duplicate","load_status","load_type_name","load_create_user_id","start_dtm","end_dtm","manual_intervention_reason"]
        }
    ]

    append_calls = append_mock.call_args_list
    assert len(append_calls) >= len(expected_append), \
        f"append_df_to_table called {len(append_calls)} times, expected >= {len(expected_append)}"

    for i, expected in enumerate(expected_append):
        args, _ = append_calls[i]
        actual_df, actual_table, actual_join = args[:3]

        # table name
        assert actual_table == expected["table"], f"Append[{i}] table mismatch: {actual_table} != {expected['table']}"

        # join keys (allow []/None flexibility if needed)
        exp_join = expected["join_on"]
        if exp_join is None:
            pass
        else:
            actual_join_norm = list(actual_join) if actual_join is not None else []
            exp_join_norm = list(exp_join)
            assert actual_join_norm == exp_join_norm, \
                f"Append[{i}] join keys mismatch: {actual_join_norm} != {exp_join_norm}"

        # dataframe contents (subset of stable columns)
        assert_dataframe_equal_unordered(actual_df, expected["df"], cols=expected.get("cols"))

    # -------------------------
    # Sanity checks
    # -------------------------
    assert call('test_catalog.example_schema.config_table') in spark_mock.table.call_args_list
    assert call('test_catalog.example_schema.file_list') in spark_mock.table.call_args_list
    assert spark_mock.read.csv.called
    adls_detect_encoding_mock.assert_any_call('csv', header_path)
    config_schema_update_mock.assert_called_once()
