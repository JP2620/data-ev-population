with source as (
    select
        value,
        el_batch_id,
        el_loaded_timestamp,
        el_loaded_date,
        el_input_filename
    from {{ source('landing', 'ev_raw_data') }}
),

result as (
    select
        (value->>'vin_1_10')::varchar(10) as vin_1_10,
        (value->>'county')::varchar(100) as county,
        (value->>'city')::varchar(100) as city,
        (value->>'state')::varchar(2) as state,
        (value->>'model_year')::integer as model_year,
        (value->>'make')::varchar(100) as make,
        (value->>'model')::varchar(100) as model,
        (value->>'ev_type')::varchar(50) as ev_type,
        (value->>'cafv_type')::varchar(100) as cafv_type,
        (value->>'electric_range')::integer as electric_range,
        (value->>'dol_vehicle_id')::bigint as dol_vehicle_id,
        (value->>'electric_utility')::varchar(200) as electric_utility,
        (value->>'legislative_district')::integer as legislative_district,
        (value->>'zip_code')::varchar(10) as zip_code,
        (value->>'_2020_census_tract')::varchar(20) as census_tract_2020,
        (value->>'geocoded_column')::text as geocoded_column,
        (value->>'socrata_id')::varchar(50) as socrata_id,
        (value->>'socrata_updated_at')::bigint as socrata_updated_at,
        el_batch_id::uuid as el_batch_id,
        el_loaded_timestamp::timestamp as el_loaded_timestamp,
        el_loaded_date::date as el_loaded_date,
        el_input_filename::varchar(255) as el_input_filename
    from source
)

select
    vin_1_10,
    county,
    city,
    state,
    model_year,
    make,
    model,
    ev_type,
    cafv_type,
    electric_range,
    dol_vehicle_id,
    electric_utility,
    legislative_district,
    zip_code,
    census_tract_2020,
    geocoded_column,
    socrata_id,
    socrata_updated_at,
    el_batch_id,
    el_loaded_timestamp,
    el_loaded_date,
    el_input_filename
from result
