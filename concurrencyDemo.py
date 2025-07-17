from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
import time

# Create a SparkSession
# In Databricks, this is already done for you as `spark`
spark = SparkSession.builder.appName("ForeachExample").getOrCreate()

# ======================================================================
# Step 1: Define Your Data Loading Function with a Closure
# ======================================================================

# This is the outer function that takes your external parameters.
# It returns the inner function that Spark will call.
def create_load_processor(param1, param2, param3, param4):
    """
    Creates and returns a function to be used by foreach.
    It "captures" the external parameters via a closure.
    """
    
    # This is the inner function that will be executed by Spark on each row.
    # It must take only a single argument (the row).
    # It can access the parameters from the outer function's scope.
    def process_single_load(row):
        try:
            # Your data loading logic here
            source_path = row.source_path
            target_table = row.target_table
            
            print(f"Starting load for: {source_path} -> {target_table} with params: {param1}, {param2}, {param3}, {param4}...")

            # --- Dummy Code to simulate a long-running load ---
            time.sleep(10) 
            # --- End Dummy Code ---

            print(f"✅ SUCCESS: Loaded {source_path} using params: {param1}, {param2}, {param3}, {param4}")
        
        except Exception as e:
            # It is crucial to use a try-except block
            print(f"❌ ERROR: Failed to load {source_path}. Reason: {e}")

    return process_single_load


# ======================================================================
# Step 2: Prepare Your Config DataFrame and Call the Function
# ======================================================================

# Your project's config table will be read here.
# For this example, we create a dummy DataFrame.
config_data = []
for i in range(1, 11): # Use a smaller number for a quick test
    config_data.append(Row(source_path=f"s3://my-bucket/files/file_{i}.parquet", 
                           target_table=f"bronze.table_{i}"))

config_df = spark.createDataFrame(config_data)

# Repartitioning is still a critical step for concurrency.
repartitioned_config_df = config_df.repartition(config_df.count())


# These are the 4 parameters you want to pass to every single load task.
my_param_a = "param_a_value"
my_param_b = 123
my_param_c = True
my_param_d = {"key": "value"}

# Create the function to be passed to foreach by calling the outer function.
# The parameters are "captured" here and sent to the executors.
parallel_function = create_load_processor(my_param_a, my_param_b, my_param_c, my_param_d)

print("--- Starting Concurrent Data Loads using foreach ---")
start_time = time.time()

# Now pass the returned inner function to foreach.
repartitioned_config_df.foreach(parallel_function)

end_time = time.time()
print(f"\n--- All tasks completed in {end_time - start_time:.2f} seconds ---")
