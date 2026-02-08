select
    {{ dbt_utils.generate_surrogate_key([
        'county', 'city', 'state', 'zip_code',
        'legislative_district', 'census_tract_2020'
    ]) }} as location_key,
    county,
    city,
    state,
    zip_code,
    legislative_district,
    census_tract_2020,
    max(latitude) as latitude,
    max(longitude) as longitude
from {{ ref('_int_ev_current_snapshot') }}
group by
    county,
    city,
    state,
    zip_code,
    legislative_district,
    census_tract_2020
