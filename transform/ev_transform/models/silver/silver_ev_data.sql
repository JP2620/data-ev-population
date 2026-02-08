{{
    config(
        unique_key='dol_vehicle_id',
        incremental_strategy='delete+insert'
    )
}}

with ranked_recency as (
    select
        *,
        row_number() over (
            partition by dol_vehicle_id
            order by socrata_updated_at desc, el_loaded_timestamp desc
        ) as row_num
    from {{ ref('bronze_ev_data') }}
    where 1=1

    {% if is_incremental() %}
        and dol_vehicle_id in (
            select dol_vehicle_id
            from {{ ref('bronze_ev_data') }}
            where el_loaded_timestamp > (select max(el_loaded_timestamp) from {{ this }})
        )
    {% endif %}
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
from ranked_recency
where row_num = 1
