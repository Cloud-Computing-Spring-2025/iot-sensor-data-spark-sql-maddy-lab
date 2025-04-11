
# 📊 IoT Sensor Data Analysis using Spark SQL

This project focuses on analyzing time-series IoT sensor data using PySpark and Spark SQL. It involves multiple steps ranging from data loading and basic exploration to advanced operations such as window functions and pivot tables.

---

## 📁 Dataset Description

The dataset used is a CSV file named `sensor_data.csv`, with simulated IoT sensor readings. Each row in the dataset contains:

| Column Name  | Description |
|--------------|-------------|
| `sensor_id`  | Unique identifier for each sensor device |
| `timestamp`  | Date and time of the sensor reading |
| `temperature`| Temperature recorded by the sensor |
| `humidity`   | Humidity percentage recorded |
| `location`   | Physical location of the sensor (e.g., BuildingA_Floor1) |
| `sensor_type`| Type/category of the sensor (e.g., TypeA, TypeB) |

---

## ⚙️ Environment Setup

Ensure the following tools/libraries are installed:

```bash
pip install pyspark
pip install faker  # Only needed if using the data generator script
```

Make sure you also have:
- Python 3.7 or above
- Java 8+
- Apache Spark set up and available in your environment

---

## ▶️ How to Run the Project

```bash
spark-submit io_sensor_analysis.py
```

This will execute all tasks and save results in the `output/` directory.

---

## 🧠 Task-by-Task Breakdown

### ✅ Task 1: Load & Basic Exploration

**Objective:**
- Load CSV data into Spark.
- Perform basic inspection.

**Steps:**
1. Define a schema for loading the data.
2. Load the CSV file with `spark.read.csv()`.
3. Create a temporary SQL view for running SQL queries.
4. Print the first 5 records using `.show()`.
5. Count the number of rows with `.count()`.
6. Find all distinct locations using `.distinct()`.

**Code Highlight:**

```python
df = spark.read.csv("sensor_data.csv", header=True, schema=schema)
df.createOrReplaceTempView("sensor_readings")
df.show(5)
df.select("location").distinct().show()
```

**Output:**
Top 5 rows are saved to `task1_output.csv`.

---

### ✅ Task 2: Filtering & Aggregation

**Objective:**
- Filter data based on temperature ranges.
- Aggregate temperature and humidity by location.

**Steps:**
1. Use `.filter()` to exclude temperatures outside the 18–30°C range.
2. Use `.groupBy()` and `.agg()` to calculate average temperature and humidity per location.
3. Sort the result by average temperature to identify the hottest location.

**Code Highlight:**

```python
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
agg_df = df.groupBy("location").agg(avg("temperature"), avg("humidity"))
```

**Output:**
Aggregated data saved to `task2_output.csv`.

---

### ✅ Task 3: Time-Based Analysis

**Objective:**
- Convert timestamp strings into Spark’s timestamp type.
- Group readings by hour of the day and compute average temperatures.

**Steps:**
1. Use `to_timestamp()` to convert the `timestamp` column.
2. Use `hour()` to extract hour-of-day.
3. Group by `hour_of_day` and calculate average temperature.

**Code Highlight:**

```python
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("hour_of_day", hour("timestamp"))
hourly_avg = df.groupBy("hour_of_day").agg(avg("temperature"))
```

**Output:**
Hourly averages saved to `task3_output.csv`.

---

### ✅ Task 4: Window Function - Sensor Ranking

**Objective:**
- Rank sensors based on their average recorded temperature.

**Steps:**
1. Group by `sensor_id` to calculate average temperature.
2. Use `dense_rank()` with a window spec ordered by average temperature in descending order.
3. Select the top 5 sensors based on rank.

**Code Highlight:**

```python
sensor_avg_temp = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))
window_spec = Window.orderBy(col("avg_temp").desc())
ranked = sensor_avg_temp.withColumn("rank_temp", dense_rank().over(window_spec))
```

**Output:**
Top 5 sensors saved to `task4_output.csv`.

---

### ✅ Task 5: Pivot & Heatmap-like Table

**Objective:**
- Create a pivot table to visualize how temperature varies by hour and location.

**Steps:**
1. Extract `hour_of_day` from the timestamp.
2. Use `.groupBy().pivot()` to create a pivot table with `location` as rows and `hour_of_day` as columns.
3. Fill values with average temperature per hour per location.

**Code Highlight:**

```python
pivot_df = df.groupBy("location").pivot("hour_of_day", list(range(24)))              .agg(round(avg("temperature"), 2))
```

**Output:**
Pivot table saved to `task5_output.csv`.

---

## 📁 Output Files

All output CSV files will be saved to the `/output` directory.

```
output/
├── task1_output.csv
├── task2_output.csv
├── task3_output.csv
├── task4_output.csv
├── task5_output.csv
```

---

## 🧪 Sample Use Cases

- **Smart Building Monitoring**: Use the analysis to determine optimal HVAC schedules.
- **Sensor Validation**: Identify malfunctioning sensors with out-of-range readings.
- **Energy Efficiency**: Target high-temp hours/locations for energy-saving interventions.

---

## 📬 Questions or Contributions?

Feel free to fork the repo, submit a pull request, or open an issue!
