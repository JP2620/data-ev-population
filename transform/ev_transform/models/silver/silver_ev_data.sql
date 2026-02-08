with latest_batch AS (
    select max(el_loaded_timestamp) as max_loaded_timestamp
    from {{ ref('bronze_ev_data') }}
),

ranked_recency as (
    select
        *,
        row_number() over (
            partition by dol_vehicle_id
            order by socrata_updated_at desc, el_loaded_timestamp desc
        ) as row_num
    from {{ ref('bronze_ev_data') }}
),

deduped as (
    SELECT
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
    FROM ranked_recency
    WHERE row_num = 1
),

final as (
    SELECT
        dd.*,
        dd.el_loaded_timestamp = lb.max_loaded_timestamp AS is_current
    FROM deduped AS dd
    CROSS JOIN latest_batch AS lb
)

select
    *
from final
