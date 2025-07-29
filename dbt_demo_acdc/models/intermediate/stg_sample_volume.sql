-- This model is materialized as a physical table by default
-- It writes new Parquet files to your clean S3 bucket

{{ config(materialized='table') }}


-- This part *transforms* the data that was read.
with cleaned as (

    select sample_id, patient_id, volume_ul,
        '{{ var("release_version") }}' as release_version
    from {{ ref('stg_sample_raw') }}
)

-- This selects the final, cleaned data to be saved.
select * from cleaned
