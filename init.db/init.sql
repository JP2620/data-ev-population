CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE IF NOT EXISTS landing.ev_raw_data (
    value JSONB,
    el_loaded_timestamp TIMESTAMP,
    el_loaded_date DATE,
    el_input_filename VARCHAR(255),
    el_batch_id UUID
);

