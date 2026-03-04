# Module 6 Homework: Spark Batch Processing
This repository contains my solutions for Module 6 of the Data Engineering Zoomcamp by DataTalksClub. The homework focuses on Apache Spark fundamentals using PySpark to process New York Yellow Taxi trip data from November 2025.

---

## Homework Questions & Solutions

**Question 1**: Spark Installation & Version Check

**Task:** Install Spark, create a local session, and check the version.

**Solution**:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Module6") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)
```
**Output:** Spark version: 3.5.0

**Question 2:** Parquet File Partitioning

**Task:** Repartition the data into 4 partitions and calculate average file size.

**Solution:**

```python
df_repart = df.repartition(4)
df_repart.write.mode("overwrite").parquet(output_path)

# Calculate average file size
parquet_files = glob.glob(f"{output_path}/*.parquet")
sizes_mb = [os.path.getsize(f)/1024/1024 for f in parquet_files]
avg_size = sum(sizes_mb)/len(sizes_mb)
```
**Answer:** 25MB average file size

**Question 3:** Trip Count for November 15

**Task:** Count trips that started on November 15, 2025.

**Solution:**

```python
from pyspark.sql.functions import to_date

df_15 = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15")
count_15 = df_15.count()
Answer: 162,604 trips
```
**Question 4:** Longest Trip Duration

**Task:** Find the maximum trip duration in hours.

**Solution:**

```python
from pyspark.sql.functions import unix_timestamp

df_with_duration = df.withColumn(
    "duration_hours",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
)

longest_trip = df_with_duration.select(spark_max("duration_hours")).collect()[0][0]
```
**Answer:** 90.6 hours

**Question 5:** Spark UI Port

**Task:** Identify the local port where Spark's web UI runs.

**Answer:** 4040 - The Spark application dashboard is available at http://localhost:4040 during job execution.

**Question 6:** Least Frequent Pickup Zone

**Task:** Join with zone lookup data to find the least frequent pickup location.

**Solution:**

```python
# Load zone lookup data
zones = spark.read.csv(f"{data_dir}/taxi_zone_lookup.csv", header=True)

# Join and aggregate
df_with_zones = df.join(zones, df.PULocationID == zones.LocationID, how="left")
least_zone = df_with_zones.groupBy("Zone").count().orderBy("count").first()
```
**Answer:** Governor's Island/Ellis Island/Liberty Island
