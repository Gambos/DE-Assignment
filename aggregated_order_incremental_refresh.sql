MERGE INTO de_assignment.aggregated_order AS target
USING (
  WITH affected_orders AS (
    SELECT DISTINCT order_id
    FROM de_assignment.order_events
    WHERE ingested_timestamp_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  ),
  full_history AS (
    SELECT 
      oe.order_id
      , oe.order_status
      , oe.event_timestamp_utc
      , oe.customer_id
      , oe.country
      , oe.amount_eur
      , oe.amount_original
      , oe.currency_original
      , oe.fx_rate
      , oe.ingested_timestamp_utc
      , MIN(oe.event_timestamp_utc) OVER (PARTITION BY oe.order_id) AS created_at
      , RANK() OVER (PARTITION BY oe.order_id ORDER BY oe.ingested_timestamp_utc DESC) AS rn
    FROM de_assignment.order_events oe
    INNER JOIN affected_orders ao ON oe.order_id = ao.order_id
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
  FROM full_history
  WHERE rn = 1
) AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.ingested_timestamp_utc > target.ingested_timestamp_utc THEN
  UPDATE SET
      order_status = source.order_status
    , event_timestamp_utc = source.event_timestamp_utc
    , customer_id = source.customer_id
    , country = source.country
    , amount_eur = source.amount_eur
    , amount_original = source.amount_original
    , currency_original = source.currency_original
    , fx_rate = source.fx_rate
    , ingested_timestamp_utc = source.ingested_timestamp_utc

WHEN NOT MATCHED THEN
  INSERT (
    order_id, order_status, event_timestamp_utc, customer_id, country,
    amount_eur, amount_original, currency_original, fx_rate,
    ingested_timestamp_utc, created_at
  )
  VALUES (
    source.order_id, source.order_status, source.event_timestamp_utc, source.customer_id, source.country,
    source.amount_eur, source.amount_original, source.currency_original, source.fx_rate,
    source.ingested_timestamp_utc, source.created_at
  );