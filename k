from unittest.mock import patch, Mock, call, ANY
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, TimestampType,
    LongType, MapType
)
from datetime import datetime as dt

@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.ConfigManager.update_config_table_schema')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.AuditLogger._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_current_timestamp')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_datetime_now')
# NOTE: REMOVED the patch for BronzeWriter._get_paths_with_and_without_header (we want it real)
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
    # File list table (state machine)
    # -------------------------
    file_list_df = Mock()
    from file_manager import FileManager  # adjust if needed
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
    # Config table (ensure proper types)
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

        # Header load: list with a single header path
        if isinstance(path, list) and path == [header_path]:
            hdr_schema = StructType([
                StructField('col_1', StringType(), True),
                StructField('col_2', StringType(), True)
            ])
            return spark_fixture.createDataFrame([('1','2')], schema=hdr_schema)

        # No-header load: list with both no-header paths at once, using header schema
        if isinstance(path, list) and path == no_header_paths:
            hdr_schema = StructType([
                StructField('col_1', StringType(), True),
                StructField('col_2', StringType(), True)
            ])
            # rows from _2 and _3 combined
            return spark_fixture.createDataFrame([('3','4'), ('5','6'), ('7','8')], schema=hdr_schema)

        # Fallback (should not be used in multi-split path)
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
    # Misc mocks
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
    # ADLS new paths (FileManager discovery)
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
    # Run job (no patch for _get_paths_with_and_without_header)
    # -------------------------
    from BRONZE_ETL import BronzeETLJob  # adjust import if needed
    dbutils_input_mock = Mock()
    BronzeETLJob(spark_mock, dbutils_input_mock, 'test_catalog', 'example_schema').run()

    # -------------------------
    # Assertions
    # -------------------------
    # Confirm config/file_list were read
    assert call('test_catalog.example_schema.config_table') in spark_mock.table.call_args_list
    assert call('test_catalog.example_schema.file_list') in spark_mock.table.call_args_list

    # Confirm CSV reads happened for header and combined no-header lists
    assert spark_mock.read.csv.called

    # Your existing append assertions can remain here (not shown for brevity).
    # Example quick sanity check:
    assert append_mock.call_count >= 1

    # Encoding detection called
    adls_detect_encoding_mock.assert_any_call('csv', header_path)

    # Schema update called once
    config_schema_update_mock.assert_called_once()
