select distinct
    {{ dbt_utils.generate_surrogate_key([
        'make', 'model', 'model_year',
        'ev_type', 'cafv_type', 'electric_range'
    ]) }} as vehicle_key,
    make,
    model,
    model_year,
    ev_type,
    cafv_type,
    electric_range
from {{ ref('_int_ev_current_snapshot') }}
