-- This model is materialized as a physical table by default
-- It writes new Parquet files to your clean S3 bucket

{{ config(materialized='view') }}

-- This part *reads* data from an external source.
with source as (

    select * from {{ source('glue_db_acdc_dbt_test_raw', 'patient_info') }}

),

cleaned as (

    select
        sample_id,
        -- Standardize storage_medium to uppercase AIR/NITROGEN or NULL
        CASE
            WHEN UPPER(TRIM(storage_medium)) = 'AIR' THEN 'AIR'
            WHEN UPPER(TRIM(storage_medium)) = 'NITROGEN' THEN 'NITROGEN'
            ELSE NULL
        END AS storage_medium,
        storage_date,
        '{{ var("release_version") }}' as release_version
    from {{ ref('stg_sample_raw') }}
)

-- This selects the final, cleaned data to be saved.
select * from cleaned
