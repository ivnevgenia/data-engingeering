# Module 3 Homework — Yellow Taxi Data 2024

This repository contains my solutions for Module 3 Homework using **ClickHouse** as an alternative to BigQuery. All queries are written in SQL and mirror the BigQuery workflow.

## Why ClickHouse Instead of BigQuery

BigQuery is not available in my current region, so ClickHouse was used as an alternative analytical database for completing this homework. ClickHouse is also a columnar database and supports partitioning, ordering, and efficient column-level scans, which makes it suitable for reproducing BigQuery concepts such as materialized tables, external tables, partition pruning, and clustering behavior. All queries and explanations follow BigQuery logic, while ClickHouse was used only as the execution engine.

## Table of Contents

1. [Setup](#setup)
2. [Answers](#answers)
3. [ClickHouse Notes](#clickhouse-notes)

---

## Setup

1. **Docker Command to Run ClickHouse**

```bash
docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 -v C:\study\yellow:/data clickhouse/clickhouse-server 
```
Connect to ClickHouse Client
```bash
docker exec -it clickhouse-server clickhouse-client
```
Create Database and Table
```sql
CREATE DATABASE IF NOT EXISTS yellow_taxi_db;
USE yellow_taxi_db;

CREATE TABLE IF NOT EXISTS yellow_taxi
(
    VendorID UInt8,
    tpep_pickup_datetime DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Float32,
    PULocationID UInt16,
    DOLocationID UInt16,
    fare_amount Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(tpep_dropoff_datetime)
ORDER BY (VendorID, tpep_dropoff_datetime);
Load Parquet Files (January–June 2024)
INSERT INTO yellow_taxi
FROM INFILE '/data/yellow_tripdata_2024-01.parquet' FORMAT Parquet;
```
-- Repeat for months 02 to 06
## Answers
1. Total Number of Records
```sql
SELECT count(*) AS total_rows
FROM yellow_taxi;
```
Answer: 20,332,093 

2. Count Distinct PULocationIDs
```sql
SELECT count(DISTINCT PULocationID) AS unique_pickup_locations
FROM yellow_taxi;
```
Answer: 18.82 MB for the External Table and 47.60 MB for the Materialized Table 

Explanation: External Table reads only the requested column; Materialized Table scans the same column from already loaded data.

3. Bytes Difference When Selecting Columns
```sql
SELECT PULocationID FROM yellow_taxi;
SELECT PULocationID, DOLocationID FROM yellow_taxi;
```
Answer:
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

4. Records with fare_amount = 0
```sql
SELECT count(*) AS zero_fare_count
FROM yellow_taxi
WHERE fare_amount = 0;
```
Answer: 8,333 

5. Optimized Table Strategy
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID 

ClickHouse Equivalent:
CREATE TABLE yellow_taxi_opt
ENGINE = MergeTree()
PARTITION BY toYYYYMM(tpep_dropoff_datetime)
ORDER BY (VendorID, tpep_dropoff_datetime);

6. Distinct VendorIDs for 2024-03-01 to 2024-03-15
```sql
SELECT count(DISTINCT VendorID)
FROM yellow_taxi_opt
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table 

7. External Table Data Storage

Answer: GCP Bucket 

8. Always Cluster Data?

Answer: False

Explanation: Clustering helps optimize queries on large tables, but it is not always necessary for smaller datasets.
ClickHouse Notes
ClickHouse is columnar, so queries behave similarly to BigQuery.
Partitioning and ORDER BY in MergeTree act like BigQuery partitions and clusters.
File(Parquet) ENGINE can simulate External Table behavior.
Queries 1–6 were executed locally on ClickHouse for verification.
