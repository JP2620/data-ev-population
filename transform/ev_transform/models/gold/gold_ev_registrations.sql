with latest_batch as (
    select max(el_loaded_timestamp) as max_loaded_timestamp
    from {{ ref('silver_ev_data') }}
),

source as (
    select
        s.*,
        s.el_loaded_timestamp = lb.max_loaded_timestamp as is_current
    from {{ ref('silver_ev_data') }} as s
    cross join latest_batch as lb
)

select
    dol_vehicle_id,
    vin_1_10,
    coalesce(nullif(county, ''), 'Unknown') as county,
    coalesce(nullif(city, ''), 'Unknown') as city,
    state,
    zip_code,
    legislative_district,
    census_tract_2020,
    model_year,
    make,
    model,
    ev_type,
    case
        when cafv_type = 'Clean Alternative Fuel Vehicle Eligible' then 'Eligible'
        when cafv_type = 'Not eligible due to low battery range' then 'Not Eligible'
        else 'Unknown'
    end as cafv_type,
    electric_range,
    electric_utility,
    is_current,
    split_part(trim(both '()' from replace(geocoded_column, 'POINT ', '')), ' ', 2)::numeric as latitude,
    split_part(trim(both '()' from replace(geocoded_column, 'POINT ', '')), ' ', 1)::numeric as longitude
from source
where is_current = true
