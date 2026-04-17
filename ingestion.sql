CREATE TABLE IF NOT EXISTS de_assignment.order_events(
  order_id string NOT NULL,
  order_status string NOT NULL,
  event_timestamp_utc timestamp NOT NULL,
  event_timestamp_local timestamp,
  customer_id string,
  country string,
  amount_eur numeric,
  amount_original numeric,
  currency_original string,
  fx_rate numeric,
  ingested_timestamp_utc timestamp NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp_utc, DAY)
CLUSTER BY order_id,order_status,country

-- Later I realized I need no event_timestamp_local column as bigquery will convert every timestamp with timezone to UTC so I dropped it
ALTER TABLE de_assignment.order_events DROP COLUMN event_timestamp_local