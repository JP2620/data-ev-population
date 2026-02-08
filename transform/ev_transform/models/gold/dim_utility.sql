select distinct
    {{ dbt_utils.generate_surrogate_key(['electric_utility']) }} as utility_key,
    electric_utility
from {{ ref('_int_ev_current_snapshot') }}
