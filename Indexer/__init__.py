import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
from elasticsearch import Elasticsearch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
indexing_subscription_name = "indexing-tasks-topic-sub"
subscriber = pubsub_v1.SubscriberClient()
indexing_subscription_path = subscriber.subscription_path(project_id, indexing_subscription_name)

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = "cse354-project-storage"
bucket = storage_client.bucket(bucket_name)

# Elasticsearch setup
es = Elasticsearch(
    ["https://your-deployment.es.us-central1.gcp.cloud.es.io:9243"],
    api_key="your-elastic-api-key"
)
index_name = "webpages"

def indexer_process():
    logging.info("Indexer node started")

    # Create index if it doesn’t exist
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "content": {"type": "text"}
                }
            }
        })

    def callback(message):
        task = json.loads(message.data.decode('utf-8'))
        url = task["url"]
        gcs_path = task["gcs_path"]
        logging.info(f"Received indexing task for {url}")

        try:
            # Download content from GCS
            blob_name = gcs_path.replace(f"gs://{bucket_name}/", "")
            blob = bucket.blob(blob_name)
            content = blob.download_as_text()
            
            # Index in Elasticsearch
            es.index(index=index_name, body={
                "url": url,
                "content": content
            })
            logging.info(f"Indexed content for {url}")
            
            message.ack()
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}")
            message.nack()

    # Subscribe to indexing tasks
    streaming_pull_future = subscriber.subscribe(indexing_subscription_path, callback=callback)
    logging.info(f"Listening for indexing tasks on {indexing_subscription_path}...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == '__main__':
    indexer_process()
