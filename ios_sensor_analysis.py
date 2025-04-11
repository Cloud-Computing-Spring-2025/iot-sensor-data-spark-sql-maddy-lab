import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, avg, hour, to_timestamp, dense_rank, round as spark_round
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Data Analysis").getOrCreate()

# Define paths
input_csv = "sensor_data.csv"  # Change path if needed
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# Define schema
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("location", StringType(), True),
    StructField("sensor_type", StringType(), True)
])

# Load CSV
df = spark.read.csv(input_csv, header=True, schema=schema)
df.createOrReplaceTempView("sensor_readings")

# ---------------- TASK 1 ----------------
# Show 5 rows
df.show(5)

# Count total records
print("Total Records:", df.count())

# Distinct locations
df.select("location").distinct().show()

# Save sample output
df.limit(5).toPandas().to_csv(f"{output_dir}/task1_output.csv", index=False)

# ---------------- TASK 2 ----------------
# Filtering
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
out_of_range = df.filter((df.temperature < 18) | (df.temperature > 30))
print("In-range count:", in_range.count())
print("Out-of-range count:", out_of_range.count())

# Aggregation
agg_df = df.groupBy("location") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ).orderBy("avg_temperature", ascending=False)

agg_df.toPandas().to_csv(f"{output_dir}/task2_output.csv", index=False)

# ---------------- TASK 3 ----------------
# Convert to timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df.createOrReplaceTempView("sensor_readings")

# Extract hour and group
hourly_avg = df.withColumn("hour_of_day", hour("timestamp")) \
    .groupBy("hour_of_day") \
    .agg(avg("temperature").alias("avg_temp")) \
    .orderBy("hour_of_day")

hourly_avg.toPandas().to_csv(f"{output_dir}/task3_output.csv", index=False)

# ---------------- TASK 4 ----------------
# Average per sensor
sensor_avg_temp = df.groupBy("sensor_id") \
    .agg(avg("temperature").alias("avg_temp"))

window_spec = Window.orderBy(col("avg_temp").desc())
ranked_sensors = sensor_avg_temp.withColumn("rank_temp", dense_rank().over(window_spec))

ranked_sensors.orderBy("rank_temp").limit(5).toPandas().to_csv(f"{output_dir}/task4_output.csv", index=False)

# ---------------- TASK 5 ----------------
# Add hour column
df = df.withColumn("hour_of_day", hour("timestamp"))

# Pivot
pivot_df = df.groupBy("location") \
    .pivot("hour_of_day", list(range(24))) \
    .agg(spark_round(avg("temperature"), 2)) \
    .orderBy("location")

pivot_df.toPandas().to_csv(f"{output_dir}/task5_output.csv", index=False)

# Done
print("All tasks completed. Output files are saved in the 'output' folder.")
