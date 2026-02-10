WITH source AS (
    SELECT * FROM {{ source('raw', 'fhv_tripdata') }}
),

filtered AS (
    SELECT
        *
    FROM source
    WHERE dispatching_base_num IS NOT NULL
),

renamed AS (
    SELECT
        cast(dispatching_base_num as varchar) AS dispatching_base_num,
        cast(pickup_datetime as timestamp) AS pickup_datetime,
        cast(dropOff_datetime as timestamp) AS dropoff_datetime,
        cast(PUlocationID as int) AS pickup_location_id,
        cast(DOlocationID as int) AS dropoff_location_id,
        cast(SR_Flag as varchar) AS sr_flag,
        cast(Affiliated_base_number as varchar) AS affiliated_base_number
    FROM filtered
)

SELECT * FROM renamed