-- This model is materialized as a physical table by default
-- It writes new Parquet files to your clean S3 bucket

{{ config(materialized='view') }}


WITH cleaned as (

    select
        sample_id,
        CASE
            WHEN UPPER(TRIM(storage_medium)) = 'AIR' THEN 'AIR'
            WHEN UPPER(TRIM(storage_medium)) = 'NITROGEN' THEN 'NITROGEN'
            ELSE NULL
        END AS storage_medium,
        storage_date,
        '{{ var("release_version") }}' as release_version,
        '{{ var("run_id") }}' as run_id 
    from {{ ref('stg_sample_raw') }}
)
select * from cleaned
