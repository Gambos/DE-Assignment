import json
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery

transformed_rows = []
pending_messages = [] 
FX_TO_EUR = {"EUR": 1.0, "GBP": 1.17, "TRY": 0.028, "USD": 0.93} # Hardcoded in this example

# Define transformation function to process pulled data and transform it to target schema
def transform(record: dict) -> dict:
    dt = datetime.fromisoformat(record["event_timestamp"])
    if dt.tzinfo is None: # Add error handling for missing timezone info in event_timestamp
        raise ValueError(f"Timestamp missing timezone: {record['event_timestamp']}")
    return {
        "order_id":               record.get("order_id"),
        "order_status":           record.get("order_status"),
        "event_timestamp_utc":    dt.astimezone(timezone.utc).isoformat(),
        "customer_id":            record.get("customer_id"),
        "country":                record.get("country"),
        "amount_eur":             round(record.get("amount", 0) * FX_TO_EUR.get(record.get("currency", "EUR"), 1.0), 2), # Convert to EUR using fx_rate
        "amount_original":        record.get("amount"),
        "currency_original":      record.get("currency"),
        "fx_rate":                FX_TO_EUR.get(record.get("currency", "EUR"), 1.0),
        "ingested_timestamp_utc": datetime.now(timezone.utc).isoformat() # Get ingested timestamp
    }

# Call Google Cloud Pub/Sub API and process messages from my subscription, based on https://docs.cloud.google.com/python/docs/reference/pubsub/latest#:~:text=publishing%20documentation.-,Subscribing,-To%20subscribe%20to
subscription_name = 'projects/{project_id}/subscriptions/{subscription_id}'.format(
    project_id='project-5f43db62-1d2f-4b07-905',
    subscription_id='DE-Assignment-sub'
)

def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        transformed_rows.append(transform(record))
        print(f"Received and transformed message: {record.get('order_id')}")
        pending_messages.append(message) # Store message for later acknowledgment after successful insertion to BigQuery
    except Exception as e:
        print(f"Nacking message: {e}")
        message.nack() # Ask Pub/Sub to redeliver message if transformation fails

with pubsub_v1.SubscriberClient() as subscriber:
    future = subscriber.subscribe(subscription_name, callback)
    try:
        future.result(timeout=60) # set up timeout for this simple demo
    except TimeoutError:
        future.cancel()

# Write post-transformed data into BigQuery with error handling, based on https://docs.cloud.google.com/bigquery/docs/json-data#:~:text=json.dumps(10)%0A%0A...-,Use%20the%20legacy%20streaming%20API,-The%20following%20example
client = bigquery.Client()
table_ref = client.dataset('de_assignment').table('order_events')
errors = client.insert_rows_json(table=table_ref, json_rows=transformed_rows)
if errors == []:
    for msg in pending_messages:
        msg.ack() # Acknowledge messages after successful insertion to BigQuery
        print(f"Acked {msg.message_id}")
    print("New rows have been added.")
else:
    for msg in pending_messages:
        msg.nack() # Ask Pub/Sub to redeliver messages if there is error during ingestion to BigQuery
        print(f"Nacked {msg.message_id}")
    print("Encountered errors while inserting rows: {}".format(errors))