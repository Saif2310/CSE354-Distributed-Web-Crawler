import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
from elasticsearch import Elasticsearch
from datetime import datetime

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

# Elasticsearch setup (local instance)
es = Elasticsearch(["http://localhost:9200"])
snapshot_repository = "gcs_snapshots"

def setup_snapshot_repository():
    """Configure GCS as snapshot repository."""
    repo_body = {
        "type": "gcs",
        "settings": {
            "bucket": bucket_name,
            "base_path": "snapshots"
        }
    }
    es.snapshot.create_repository(repository=snapshot_repository, body=repo_body)
    logging.info("Configured GCS snapshot repository")

def indexer_process():
    logging.info("Indexer node started")
    setup_snapshot_repository()

    processed_crawl_ids = set()

    def callback(message):
        task = json.loads(message.data.decode('utf-8'))
        url = task["url"]
        gcs_path = task["gcs_path"]
        crawl_query_id = task["crawl_query_id"]
        index_name = f"webpages_{crawl_query_id}"
        logging.info(f"Received indexing task for {url} with crawl_query_id {crawl_query_id}")

        try:
            # Create index with mapping if it doesn’t exist
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, body={
                    "mappings": {
                        "properties": {
                            "url": {"type": "keyword"},
                            "title": {"type": "text"},
                            "content": {"type": "text"},
                            "metadata": {
                                "properties": {
                                    "description": {"type": "text"},
                                    "author": {"type": "keyword"},
                                    "publish_date": {"type": "date", "format": "yyyy-MM-dd"},
                                    "language": {"type": "keyword"},
                                    "keywords": {"type": "keyword"}
                                }
                            }
                        }
                    }
                })

            # Download content from GCS
            blob_name = gcs_path.replace(f"gs://{bucket_name}/", "")
            blob = bucket.blob(blob_name)
            content = blob.download_as_text()
            structured_data = json.loads(content)

            # Combine headings and text blocks
            headings = structured_data["main_content"]["headings"]
            heading_text = " ".join(
                headings.get("h1", []) +
                headings.get("h2", []) +
                headings.get("h3", [])
            )
            text_blocks = " ".join(structured_data["main_content"]["text_blocks"])
            content_text = heading_text + " " + text_blocks

            # Prepare document
            doc = {
                "url": structured_data["url"],
                "title": structured_data["title"],
                "content": content_text,
                "metadata": structured_data["metadata"]
            }

            # Index in Elasticsearch
            es.index(index=index_name, body=doc)
            logging.info(f"Indexed content for {url} in {index_name}")

            # Track processed crawl IDs
            processed_crawl_ids.add(crawl_query_id)

            # Take snapshot to GCS periodically or on completion
            if len(processed_crawl_ids) % 10 == 0:  # Adjust frequency as needed
                snapshot_name = f"snapshot_{crawl_query_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                es.snapshot.create(
                    repository=snapshot_repository,
                    snapshot=snapshot_name,
                    body={"indices": index_name}
                )
                logging.info(f"Created snapshot {snapshot_name} for {index_name}")

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
