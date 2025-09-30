@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.ConfigManager.update_config_table_schema')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.AuditLogger._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_current_timestamp')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.BronzeWriter._get_paths_with_and_without_header')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_datetime_now')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_current_timestamp')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.append_df_to_table')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.merge_df_to_table')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.datetime')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._adls_detect_encoding')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.FileManager._get_new_paths_list')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.DeltaManager.set_table_properties')
@patch('deltalake_dabs.src.shared.Bronze.cls_bronze_etl.WidgetManager')
def test_multi_split_csv_success(widgets_mock, set_properties_mock, new_paths_list_mock, adls_detect_encoding_mock, datetime_mock, merge_mock, append_mock, file_list_current_timestamp_mock,file_list_time_mock,get_paths_with_and_without_header_mock,bronze_writer_time_mock, bronze_writer_current_timestamp_mock, audit_logger_now_mock, config_schema_update_mock, spark_fixture):

    #Input mocks:
    #The below mocks all incoming data to the function.
    #1. dbutils.widgets
    #2. File List 
    #3. Config Table
    #4. Spark
    #5. ADLS
    #6. Timestamps

    ##################
    # Widget Values
    ##################

    #used for widget return values
    #mock will use side effect to pass its input to widget name and return
    #a value based in what it was called with
    def return_widgets_value(widget_name):
        match widget_name:
            case 'email_id':
                return 'TR_ExampleTrigger'
            case 'databricks_run_id':
                return '8507742291424483'
            case 'databricks_job_id':
                return '111222333'
            case 'adf_run_id':
                return 'corr_id_123'
            case _:
                return None

    #mock widgets to return correct values
    widgets_mock.return_value = widgets_mock
    widgets_mock.get_value.side_effect = return_widgets_value

    #Sometimes the mock uses .get_value and sometimes it uses __getattr__
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


    ##################
    # Read File List
    ##################

    #file list data needs file status to change after call to count as claimed. accomplished via side effect
    file_list_df = Mock()
    #only need the schema for this test
    file_list_schema = FileManager(spark_fixture,None,None,Mock(),None,None)._file_list_schema()
    file_list_values = [
            #Notice while this test data mimics a real file, this test never reads from a file. Instead any read returns data mimicking
            #What a real run would return by mocking/patching.
            spark_fixture.createDataFrame([('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv','FakeFile_20250129_1.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PENDING.value,dt(2025,4,2,12,0,0),None),
            ('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv','FakeFile_20250129_2.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PENDING.value,dt(2025,4,2,12,0,0),None),
            ('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv','FakeFile_20250129_3.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PENDING.value,dt(2025,4,2,12,0,0),None)],
            file_list_schema),
            spark_fixture.createDataFrame([('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv','FakeFile_20250129_1.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PROCESSING.value,dt(2025,4,2,12,0,0),None),
            ('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv','FakeFile_20250129_2.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PROCESSING.value,dt(2025,4,2,12,0,0),None),
            ('/Fiserv/FakeFile/','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv','FakeFile_20250129_3.csv',dt(2025,2,2,14,0,0),5000,'csv','utf-8','111222333','8507742291424483',FileStatus.PROCESSING.value,dt(2025,4,2,12,0,0),None)],
            file_list_schema)
    ]
    file_list_df.side_effect = file_list_values

    ##################
    # Read Config Table
    ##################

    #dataframes to be returned

    config_file_df = spark_fixture.createDataFrame([('FiServ','Demographics','demographic','bronze_fiserv','fiserv_FakeFile','/Fiserv/FakeFile/','UTF-16',"True",'csv','csv','|',"True",0.02,"2025-01-01 12:00:00")],
                                                    ('category_1','category_2','catalog_name','schema_name','target_table_name','source_folder','file_encoding_type','has_header','file_type','file_extension','column_delimiter','is_active','allowed_threshold','updated_timestamp'),
                                                    StructType([StructField('category_1', StringType(), True), 
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
                                                                StructField('updated_timestamp', TimestampType(), True)]))
    config_table_df = spark_fixture.createDataFrame([('FiServ','Demographics','demographic','bronze_fiserv','fiserv_FakeFile','/Fiserv/FakeFile/','UTF-16',True,'csv','csv','|',True,0.02,dt(2025,1,1,12,00,00))],
                                                    ('category_1','category_2','catalog_name','schema_name','target_table_name','source_folder','file_encoding_type','has_header','file_type','file_extension','column_delimiter','is_active','allowed_threshold','updated_timestamp'),
                                                    StructType([StructField('category_1', StringType(), True), 
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
                                                                StructField('updated_timestamp', TimestampType(), True)]))
    #handles which table is being read from (config or file list)
    #Please notice that these schemas do not actually exist. This test never reads from a real table
    #Instead, the process mimicks what a real run would do on dummy data.
    #Fake catalogs and schemas are used to ensure no errant writes to real tables if new code is not properly mocked
    def table_return_value(path):
        match path:
            case 'test_catalog.example_schema.config_table':
                return config_table_df
            case 'test_catalog.example_schema.file_list':
                return file_list_df()
    #handles whether read.csv is reading a file or the config file        
    def csv_return_value(path):
        match path:
            #multi-split-file
            case '/Volumes/dev_deltalake/raw/config-files/bronze_config_table.csv':
                return config_file_df
                
            #config file
            case ['dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv']:
                return spark_fixture.createDataFrame([('col_1','col_2','_metadata'),('1','2',{'file_name':'FakeFile_20250129_1.csv'})],('col_1','col_2','_metadata'))
            case 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv':
                return spark_fixture.createDataFrame([('1','2',{'file_name':'FakeFile_20250129_2.csv'}), ('1','2',{'file_name':'FakeFile_20250129_2.csv'})],('col_1','col_2','_metadata'))
            case 'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv':
                return spark_fixture.createDataFrame([('1','2',{'file_name':'FakeFile_20250129_3.csv'}), ('1','2',{'file_name':'FakeFile_20250129_3.csv'})],('col_1','col_2','_metadata'))
            case ['dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv','dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv']:
                return spark_fixture.createDataFrame([('1','2',{'file_name':'FakeFile_20250129_2.csv'}), ('1','2',{'file_name':'FakeFile_20250129_2.csv'})],[('1','2',{'file_name':'FakeFile_20250129_3.csv'}), ('1','2',{'file_name':'FakeFile_20250129_3.csv'})],('col_1','col_2','_metadata'))
            case _:
                print("Invalid Path")
                print(path)
                assert False

    #mock spark for above table reads
    spark_mock = Mock()
    spark_mock.table.side_effect = table_return_value
    spark_mock.read.option.return_value = spark_mock.read
    spark_mock.read.options.return_value = spark_mock.read
    spark_mock.read.csv.side_effect = csv_return_value
    #keep dataframe creation which is used by logging
    spark_mock.createDataFrame = spark_fixture.createDataFrame

    append_mock.return_value = {'FakeFile_20250129_1.csv':1, 'FakeFile_20250129_2.csv':2, 'FakeFile_20250129_3.csv':2}

    ##################
    # ADLS
    ##################
    
    #create a mock file to be read in ADLS
    mock_file_1 = Mock()
    mock_file_1.path = "dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv"
    mock_file_1.name = "FakeFile_20250129_1.csv"
    mock_file_1.modificationTime = dt(2025,2,2,14,00,00)
    mock_file_1.size = 5000

    mock_file_2 = Mock()
    mock_file_2.path = "dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv"
    mock_file_2.name = "FakeFile_20250129_2.csv"
    mock_file_2.modificationTime = dt(2025,2,2,14,00,00)
    mock_file_2.size = 5000

    mock_file_3 = Mock()
    mock_file_3.path = "dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv"
    mock_file_3.name = "FakeFile_20250129_3.csv"
    mock_file_3.modificationTime = dt(2025,2,2,14,00,00)
    mock_file_3.size = 5000


    #file manager returns file from ADLS 
    new_paths_list_mock.return_value = [mock_file_1, mock_file_2, mock_file_3]

    ################################
    # Datetime and Current_Timestamp
    ################################

    #mock begin and end dates. Different dates used to simulate job taking time.
    datetime_mock.now.return_value = dt(2025,2,2,12,00,00)
    datetime_mock.combine.return_value = dt(2025,3,2,12,00,00)
    #When searching ADLS
    file_list_time_mock.return_value = dt(2025,4,2,12,00,00)
    #When ingesting file
    bronze_writer_time_mock.return_value = dt(2025,4,2,12,30,00)
    #used for end_dtm when writing to run_load_history
    audit_logger_now_mock.return_value = dt(2025,4,2,12,55,00)

    #File list also uses F.current_timestamp()
    #convert datetime so local timezone does not effect
    file_list_current_timestamp_mock.return_value = F.lit(dt(2025,4,2,5,00,00)).cast('timestamp')
    bronze_writer_current_timestamp_mock.return_value = F.lit(dt(2025,4,2,5,45,00)).cast('timestamp')

    #mock encoding as there is no file to read
    adls_detect_encoding_mock.return_value = 'utf-8'

    #to pass in __init__
    dbutils_input_mock = Mock()

    get_paths_with_and_without_header_mock.return_value = ['FakeFile_20250129_1.csv'], ['FakeFile_20250129_2.csv', 'FakeFile_20250129_3.csv']



    ################################
    #         FUNCTION CALL
    ################################


    #Call the whole master bronze and set to a non-existant catalog and schema.
    #This ensures if the mock breaks there are no errant overwrites in dev
    BronzeETLJob(spark_mock, dbutils_input_mock, 'test_catalog', 'example_schema').run()


    ############################
    #-----Test Merged Data------
    ############################


    #This is the data for the write to the file list from ADLS data. It is merged three times. 
    #Once after being discovered in ADLS, once updated with the status of Processing, 
    #and once after load status has completed (or failed)
    file_list_1_df = spark_fixture.createDataFrame([{'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv',
                                                     'file_name':'FakeFile_20250129_1.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'updated_timestamp':None}, 
                                                    {'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv',
                                                     'file_name':'FakeFile_20250129_2.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'updated_timestamp':None},
                                                     {'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv',
                                                     'file_name':'FakeFile_20250129_3.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'updated_timestamp':None}],
                                                   file_list_schema)
    file_list_2_df = spark_fixture.createDataFrame([{'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv',
                                                     'file_name':'FakeFile_20250129_1.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'new_run_id':'8507742291424483',
                                                     'new_status':FileStatus.PROCESSING.value,
                                                     'new_timestamp':dt(2025,4,2,5,0,0),
                                                     'updated_timestamp':None},
                                                    {'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv',
                                                     'file_name':'FakeFile_20250129_2.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'new_run_id':'8507742291424483',
                                                     'new_status':FileStatus.PROCESSING.value,
                                                     'new_timestamp':dt(2025,4,2,5,0,0),
                                                     'updated_timestamp':None},
                                                    {'source_folder':'/Fiserv/FakeFile/',
                                                     'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv',
                                                     'file_name':'FakeFile_20250129_3.csv',
                                                     'file_modification_time':dt(2025,2,2,14,0,0),
                                                     'file_size_in_bytes':5000,
                                                     'file_extension':'csv',
                                                     'expected_encoding_type':'utf-8',
                                                     'databricks_job_id':'111222333',
                                                     'databricks_run_id':'8507742291424483',
                                                     'loaded_to_bronze':FileStatus.PENDING.value,
                                                     'created_timestamp':dt(2025,4,2,12,0,0),
                                                     'new_run_id':'8507742291424483',
                                                     'new_status':FileStatus.PROCESSING.value,
                                                     'new_timestamp':dt(2025,4,2,5,0,0),
                                                     'updated_timestamp':None}],
                                                   StructType([
                                                        StructField('created_timestamp', TimestampType(), True), 
                                                        StructField('databricks_job_id', StringType(), True), 
                                                        StructField('databricks_run_id', StringType(), True), 
                                                        StructField('expected_encoding_type', StringType(), True), 
                                                        StructField('file_extension', StringType(), True), 
                                                        StructField('file_modification_time', TimestampType(), True), 
                                                        StructField('file_name', StringType(), True), 
                                                        StructField('file_path', StringType(), True), 
                                                        StructField('file_size_in_bytes', LongType(), True), 
                                                        StructField('loaded_to_bronze', IntegerType(), True), 
                                                        StructField('new_run_id', StringType(), False), 
                                                        StructField('new_status', IntegerType(), False), 
                                                        StructField('new_timestamp', TimestampType(), False), 
                                                        StructField('source_folder', StringType(), True), 
                                                        StructField('updated_timestamp', TimestampType(), True)])
                                                   )
    file_list_3_df = spark_fixture.createDataFrame([{'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv',
                                                     'databricks_run_id':'8507742291424483',
                                                     'new_status':FileStatus.SUCCESS.value,
                                                     'new_timestamp':dt(2025,4,2,12,30,0)},
                                                    {'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_2.csv',
                                                     'databricks_run_id':'8507742291424483',
                                                     'new_status':FileStatus.SUCCESS.value,
                                                     'new_timestamp':dt(2025,4,2,12,30,0)},
                                                    {'file_path':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_3.csv',
                                                     'databricks_run_id':'8507742291424483',
                                                     'new_status':FileStatus.SUCCESS.value,
                                                     'new_timestamp':dt(2025,4,2,12,30,0)}],
                                                    StructType([
                                                        StructField('databricks_run_id', StringType(), True), 
                                                        StructField('file_path', StringType(), True), 
                                                        StructField('new_status', LongType(), True), 
                                                        StructField('new_timestamp', TimestampType(), True)])
                                                   )
    #update to the config table
    expected_merge_calls =  [
            #merge to config table
            {'source_df':config_file_df,
            'target_table':'test_catalog.example_schema.config_table', 
            'merge_condition':'src.source_folder = tgt.source_folder', 
            'update_dict': {'category_1': 'src.category_1', 
                            'category_2': 'src.category_2', 
                            'catalog_name': 'src.catalog_name', 
                            'schema_name': 'src.schema_name', 
                            'target_table_name': 'src.target_table_name', 
                            'source_folder': 'src.source_folder', 
                            'file_encoding_type': 'src.file_encoding_type', 
                            'has_header': 'src.has_header', 
                            'file_type': 'src.file_type', 
                            'file_extension': 'src.file_extension', 
                            'column_delimiter': 'src.column_delimiter', 
                            'is_active': 'src.is_active', 
                            'allowed_threshold': 'src.allowed_threshold', 
                            'updated_timestamp': 'src.updated_timestamp'}, 
            'insert_dict':ANY, 
            'update_condition':'src.updated_timestamp > tgt.updated_timestamp'},
            #file list write for new adls file
            {'source_df':file_list_1_df,
             'target_table':'test_catalog.example_schema.file_list', 
             'merge_condition':'tgt.file_name = src.file_name', 
             'update_dict':{'source_folder': 'src.source_folder', 
                            'file_path': 'src.file_path', 
                            'file_name': 'src.file_name', 
                            'file_modification_time': 'src.file_modification_time', 
                            'file_size_in_bytes': 'src.file_size_in_bytes', 
                            'file_extension': 'src.file_extension', 
                            'expected_encoding_type': 'src.expected_encoding_type', 
                            'databricks_job_id': 'src.databricks_job_id', 
                            'databricks_run_id': 'src.databricks_run_id', 
                            'loaded_to_bronze': 'src.loaded_to_bronze', 
                            'created_timestamp': 'src.created_timestamp', 
                            'updated_timestamp': 'src.updated_timestamp'}, 
             'insert_dict':{'source_folder': 'src.source_folder', 
                            'file_path': 'src.file_path', 
                            'file_name': 'src.file_name', 
                            'file_modification_time': 'src.file_modification_time', 
                            'file_size_in_bytes': 'src.file_size_in_bytes', 
                            'file_extension': 'src.file_extension', 
                            'expected_encoding_type': 'src.expected_encoding_type', 
                            'databricks_job_id': 'src.databricks_job_id', 
                            'databricks_run_id': 'src.databricks_run_id', 
                            'loaded_to_bronze': 'src.loaded_to_bronze', 
                            'created_timestamp': 'src.created_timestamp', 
                            'updated_timestamp': 'src.updated_timestamp'}},
            
            #file list update with loaded to bronze = processing
            {'source_df':file_list_2_df,
             'target_table':'test_catalog.example_schema.file_list', 
             'merge_condition':'tgt.file_path = src.file_path', 
             'update_dict':{'databricks_run_id': 'src.new_run_id', 
                            'loaded_to_bronze': 'src.new_status',
                            'updated_timestamp': 'src.new_timestamp'},
             'update_condition':'tgt.loaded_to_bronze = 0'},
            
            #File list update once record has successfully loaded
            {'source_df':file_list_3_df,
             'target_table':'test_catalog.example_schema.file_list', 
             'merge_condition':'tgt.file_path = src.file_path', 
             'update_dict':{'loaded_to_bronze': 'src.new_status',
                            'updated_timestamp': 'src.new_timestamp'},
             'update_condition':f"tgt.loaded_to_bronze = {FileStatus.PROCESSING.value} AND tgt.databricks_run_id = src.databricks_run_id"}]
    
    ##############################
    #-----Test Appended Data------
    ##############################

    #dataframes appended to bronze and run_load_history
    expected_written_df_1 = spark_fixture.createDataFrame([{'file_name':'FakeFile_20250129_1.csv',
                                                          'col_1':'1',
                                                          'col_2':'2',
                                                          '_metadata':{'file_name':'FakeFile_20250129.csv'},
                                                          'load_create_user_id':'TR_ExampleTrigger',
                                                          'databricks_job_id':'111222333',
                                                          'correlation_id':'corr_id_123',
                                                          'databricks_run_id':'8507742291424483',
                                                          'load_create_dtm':dt(2025,4,2,5,45,00),
                                                          'load_update_user_id':None,
                                                          'load_update_dtm':None}
                                                         #with column used to set datatype
                                                         ],
                                                        StructType([
                                                            StructField('_metadata', MapType(StringType(), StringType(), True), True), 
                                                            StructField('col_1', StringType(), True), StructField('col_2', StringType(), True), StructField('correlation_id', StringType(), True), StructField('databricks_job_id', StringType(), True), 
                                                            StructField('databricks_run_id', StringType(), True), 
                                                            StructField('file_name', StringType(), True), 
                                                            StructField('load_create_dtm', TimestampType(), True),
                                                            StructField('load_create_user_id', StringType(), True), 
                                                            StructField('load_update_user_id', StringType(), True), 
                                                            StructField('load_update_dtm', TimestampType(), True)]))
    expected_written_df_2 = spark_fixture.createDataFrame([{'file_name':'FakeFile_20250129_2.csv',
                                                          'col_1':'1',
                                                          'col_2':'2',
                                                          '_metadata':{'file_name':'FakeFile_20250129.csv'},
                                                          'load_create_user_id':'TR_ExampleTrigger',
                                                          'databricks_job_id':'111222333',
                                                          'correlation_id':'corr_id_123',
                                                          'databricks_run_id':'8507742291424483',
                                                          'load_create_dtm':dt(2025,4,2,5,45,00),
                                                          'load_update_user_id':None,
                                                          'load_update_dtm':None}
                                                         #with column used to set datatype
                                                         ],
                                                        StructType([
                                                            StructField('_metadata', MapType(StringType(), StringType(), True), True), 
                                                            StructField('col_1', StringType(), True), StructField('col_2', StringType(), True), StructField('correlation_id', StringType(), True), StructField('databricks_job_id', StringType(), True), 
                                                            StructField('databricks_run_id', StringType(), True), 
                                                            StructField('file_name', StringType(), True), 
                                                            StructField('load_create_dtm', TimestampType(), True),
                                                            StructField('load_create_user_id', StringType(), True), 
                                                            StructField('load_update_user_id', StringType(), True), 
                                                            StructField('load_update_dtm', TimestampType(), True)]))
    expected_written_df_3 = spark_fixture.createDataFrame([{'file_name':'FakeFile_20250129_3.csv',
                                                          'col_1':'1',
                                                          'col_2':'2',
                                                          '_metadata':{'file_name':'FakeFile_20250129.csv'},
                                                          'load_create_user_id':'TR_ExampleTrigger',
                                                          'databricks_job_id':'111222333',
                                                          'correlation_id':'corr_id_123',
                                                          'databricks_run_id':'8507742291424483',
                                                          'load_create_dtm':dt(2025,4,2,5,45,00),
                                                          'load_update_user_id':None,
                                                          'load_update_dtm':None}
                                                         #with column used to set datatype
                                                         ],
                                                        StructType([
                                                            StructField('_metadata', MapType(StringType(), StringType(), True), True), 
                                                            StructField('col_1', StringType(), True), StructField('col_2', StringType(), True), StructField('correlation_id', StringType(), True), StructField('databricks_job_id', StringType(), True), 
                                                            StructField('databricks_run_id', StringType(), True), 
                                                            StructField('file_name', StringType(), True), 
                                                            StructField('load_create_dtm', TimestampType(), True),
                                                            StructField('load_create_user_id', StringType(), True), 
                                                            StructField('load_update_user_id', StringType(), True), 
                                                            StructField('load_update_dtm', TimestampType(), True)]))
    
    expected_run_load_history_df = spark_fixture.createDataFrame([{'databricks_job_id':'111222333',
                                                                   'databricks_run_id':'8507742291424483',
                                                                   'correlation_id':'corr_id_123',
                                                                   'source_name':'dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129.csv',
                                                                   'target_name':'test_catalog.example_schema.fiserv_FakeFile',
                                                                   'load_stage_name':'RawToBronze',
                                                                   'operation_type':'Append',
                                                                    'records_total':3,
                                                                    'records_succeeded':3,
                                                                    'records_rejected':0,
                                                                    'records_deleted':0,
                                                                    'records_duplicate':0,
                                                                    'load_status':'Success',
                                                                    'load_type_name':'Pipeline',
                                                                    'load_create_user_id':'TR_ExampleTrigger',
                                                                    'end_dtm':dt(2025,4,2,12,55,00),
                                                                    'start_dtm':dt(2025,4,2,12,30,00),
                                                                    'manual_intervention_reason':None
                                                                }],
                                                                 StructType([
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
                                                                    StructField('manual_intervention_reason', StringType(), True)]))

    #check dataframe, target_table, join on
    expected_append_calls = [[expected_written_df_1,'test_catalog.example_schema.fiserv_FakeFile',['file_name'],],
                             [expected_written_df_2,'test_catalog.example_schema.fiserv_FakeFile',['file_name'],],
                             [expected_written_df_3,'test_catalog.example_schema.fiserv_FakeFile',['file_name'],],
                             [expected_run_load_history_df,'test_catalog.example_schema.run_load_history',['file_name']]]


    assert spark_mock.table.call_args_list ==  [call('test_catalog.example_schema.config_table'),
                                                call('test_catalog.example_schema.file_list'),
                                                call('test_catalog.example_schema.file_list')]

    # #Check all of the calls to merge_df_to_table
    # for i, merge_call in enumerate(merge_mock.call_args_list):
    #     #loop through kwargs. Function only uses kwargs
    #     for key, value in merge_call[1].items():
    #         if isinstance(value, DataFrame) or isinstance(value, ConnectDataFrame):
    #             #Dataframes are compared using utility function
    #             assertDataFrameEqual(value, expected_merge_calls[i][key], ignoreColumnOrder=True)
    #         else:
    #             #other kwargs use ==
    #             assert value == expected_merge_calls[i][key]

    #check all calls to append_df_to_table
    for i, append_call in enumerate(append_mock.call_args_list):
        for input_number, append_input in enumerate(append_call[0]):
            if isinstance(append_input, DataFrame) or isinstance(append_input, ConnectDataFrame):
                assertDataFrameEqual(append_input, expected_append_calls[i][input_number], ignoreColumnOrder=True)
            else:
                assert append_input == expected_append_calls[i][input_number]

    #ensure detect encoding is called correctly as it is mocked
    # adls_detect_encoding_mock.assert_any_call('csv',"dbfs:/Volumes/fake_deltalake/raw/Fiserv/FakeFile/2025/01/29/FakeFile_20250129_1.csv")
    config_schema_update_mock.assert_called_once()


