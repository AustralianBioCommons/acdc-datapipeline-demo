-- This model is materialized as a physical table by default
-- It writes new Parquet files to your clean S3 bucket

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite'
) }}

-- This part *reads* data from an external source.
with source as (

    select * from {{ source('glue_db_acdc_dbt_test_raw', 'patient_info') }}

),

-- This part *transforms* the data that was read.
cleaned as (

    select
        patient_id,
        CASE
            WHEN UPPER(TRIM(sex)) IN ('M', 'MALE', '0', 'Male') THEN 'male'
            WHEN UPPER(TRIM(sex)) IN ('F', 'FEMALE', '1', 'fem', 'Female') THEN 'female'
            ELSE NULL
        END AS biological_sex,
        '{{ var("release_version") }}' as release_version,
        '{{ var("run_id") }}' as run_id
    from source
    where patient_id <> 'patient_id'
        and sex <> 'sex'
        and patient_id is not NULL
        and patient_id <> ''
)

-- This selects the final, cleaned data to be saved.
select * from cleaned
