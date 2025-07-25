-- This model is materialized as a physical table by default
-- It writes new Parquet files to your clean S3 bucket

{{ config(materialized='view') }}

-- This part *reads* data from an external source.
with source as (

    select * from {{ source('raw_data', 'patient_info') }}

),

-- This part *transforms* the data that was read.
cleaned as (

    select
        patient_id,
        CASE
            WHEN UPPER(TRIM(sex)) IN ('M', 'MALE', '0') THEN 'male'
            WHEN UPPER(TRIM(sex)) IN ('F', 'FEMALE', '1', 'fem') THEN 'female'
            ELSE NULL
        END AS sex
    from source
)

-- This selects the final, cleaned data to be saved.
select * from cleaned
