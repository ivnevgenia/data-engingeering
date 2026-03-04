# Homework: Build Your Own dlt Pipeline

This repository contains the solution for the homework on **Build Your Own dlt Pipeline**. The task is to create a custom dlt pipeline that loads NYC taxi trip data from a custom API into DuckDB, processes the data, and answers some questions.

## Challenge Overview

- Build a data pipeline using a custom REST API source for NYC taxi trips.
- Save the data into DuckDB.
- Analyze the data to answer specific questions.

## Questions & Answers

#### Question 1: What is the start date and end date of the dataset?
#### Answer: 
2009-06-01 to 2009-07-01
```sql
SELECT MIN(trip_pickup_date_time) AS start_date, MAX(trip_pickup_date_time) AS end_date FROM "taxi_data"
```
#### Question 2: What proportion of trips are paid with credit card?
#### Answer: 
26.66%
```sql
SELECT SUM(CASE WHEN payment_type ILIKE '%credit%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS credit_card_ratio FROM "taxi_data"
```
#### Question 3: What is the total amount of tips earned?
#### Answer:
$6,063.41


```sql

SELECT SUM(tip_amt) AS total_tips FROM "taxi_data"
```
