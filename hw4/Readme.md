# Module 4 Homework: Analytics Engineering with dbt

## Overview
In this homework, we worked on a dbt project analyzing NYC taxi data. The main tasks involved setting up models, running tests, analyzing data, and creating additional models.

---

## Steps Performed

### 1. Data Preparation
- Loaded Green and Yellow taxi data for 2019-2020 into the data warehouse.
- Executed `dbt build --target prod` to build models and run tests in the production environment.

### 2. Questions and Answers

#### Question 1: What models will be built if you run `dbt run --select int_trips_unioned`?
- **Answer:** The models `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (depending on source data and dependencies).

#### Question 2: What happens when running `dbt test --select fct_trips`, if a new value `6` appears in the source data for `payment_type`?
- **Answer:** dbt will **fail** the test because the accepted values are now out of date, and the test returns a non-zero exit code.

#### Question 3: How many records are in the `fct_monthly_zone_revenue` model?
- **Answer:** 12,184 records.

#### Question 4: Which zone had the highest total revenue in 2020 among Green taxis?
- **Answer:** `East Harlem North` with approximately **1,816,942.75** in revenue.

#### Question 5: What was the total number of Green taxi trips in October 2019?
- **Answer:** 384,624 trips.

#### Question 6: Build a staging model for FHV trip data for 2019
- - Loaded FHV trip data.
- - Created a staging model `stg_fhv_tripdata`.
- - Filtered out records where `dispatching_base_num IS NULL`.
- - Renamed fields to match project conventions.
- **Answer:** The total record count is **43,244,693**.

---

## SQL Queries and Results

### Count of records in `fct_monthly_zone_revenue`
```sql
select count(1) from "taxi_rides_ny"."prod"."fct_monthly_zone_revenue";
```
-- Result: 12,184
Find the zone with the highest revenue in 2020 for Green taxis
```sql

select pickup_zone, sum(revenue_monthly_total_amount) as total_revenue_2020
from "taxi_rides_ny"."prod"."fct_monthly_zone_revenue"
where service_type = 'Green' and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 desc
limit 1;
```
-- Result: East Harlem North
Count of Green taxi trips in October 2019
```sql

select sum(total_monthly_trips) as total_trips_oct2019
from "taxi_rides_ny"."prod"."fct_monthly_zone_revenue"
where service_type = 'Green' and revenue_month = '2019-10-01';
```
-- Result: 384,624 trips
Count of records in FHV staging data
```sql
select count(1) from "taxi_rides_ny"."prod"."stg_fhv_tripdata";
```
-- Result: 43,244,693
