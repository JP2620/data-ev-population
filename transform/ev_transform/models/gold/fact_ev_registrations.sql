select
    dol_vehicle_id,
    vin_1_10,
    {{ dbt_utils.generate_surrogate_key([
        'county', 'city', 'state', 'zip_code',
        'legislative_district', 'census_tract_2020'
    ]) }} as location_key,
    {{ dbt_utils.generate_surrogate_key([
        'make', 'model', 'model_year',
        'ev_type', 'cafv_type', 'electric_range'
    ]) }} as vehicle_key,
    {{ dbt_utils.generate_surrogate_key(['electric_utility']) }} as utility_key,
    1 as registration_count
from {{ ref('_int_ev_current_snapshot') }}
