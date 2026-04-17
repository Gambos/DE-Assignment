CREATE OR REPLACE TABLE de_assignment.aggregated_order AS
WITH CTE AS (
  SELECT 
    order_id
    , order_status
    , event_timestamp_utc
    , customer_id
    , country
    , amount_eur
    , amount_original
    , currency_original
    , fx_rate
    , ingested_timestamp_utc
    , MIN(event_timestamp_utc) OVER (PARTITION BY order_id) as created_at
    , RANK() OVER (PARTITION BY order_id ORDER BY ingested_timestamp_utc DESC) as rn
  FROM de_assignment.order_events
)
  SELECT 
    order_id
    , order_status
    , event_timestamp_utc
    , customer_id
    , country
    , amount_eur
    , amount_original
    , currency_original
    , fx_rate
    , ingested_timestamp_utc
    , created_at
  FROM CTE
  WHERE rn = 1;