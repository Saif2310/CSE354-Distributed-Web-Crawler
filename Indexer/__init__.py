import json
import logging
import time
import uuid
from google.cloud import pubsub_v1
from google.cloud import storage
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError

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

# Rate limiting delay in seconds (202.08 ms = 0.20208 seconds)
RATE_LIMIT_DELAY = 0.20208

def setup_snapshot_repository():
    """Configure GCS as snapshot repository with retry logic for errors."""
    repo_settings = {
        "type": "gcs",
        "settings": {
            "bucket": bucket_name,
            "base_path": "snapshots"
        }
    }
    max_retries = 5
    for attempt in range(max_retries):
        try:
            es.snapshot.create_repository(repository=snapshot_repository, body=repo_settings)
            logging.info("Configured GCS snapshot repository")
            return
        except TransportError as e:
            if 'repository_exception' in str(e) and attempt < max_retries - 1:
                logging.warning("Repository state inconsistent during setup. Recovering repository...")
                es.snapshot.delete_repository(repository=snapshot_repository, ignore=[404])
                continue
            else:
                raise
    raise Exception(f"Failed to configure snapshot repository after {max_retries} attempts")

def recover_repository():
    """Recover the repository by deleting and re-adding it."""
    logging.warning("Repository state inconsistent. Recovering repository...")
    es.snapshot.delete_repository(repository=snapshot_repository, ignore=[404])
    setup_snapshot_repository()
    logging.info("Recovered GCS snapshot repository")

def indexer_process():
    logging.info("Indexer node started")
    setup_snapshot_repository()

    processed_crawl_ids = set()

    def callback(message):
        # Record start time when message is received
        start_time = time.time()
        
        task = json.loads(message.data.decode('utf-8'))
        url = task["url"]
        gcs_path = task["gcs_path"]
        crawl_query_id = task["crawl_query_id"]
        index_name = f"webpages_{crawl_query_id}"
        logging.info(f"Received indexing task for {url} with crawl_query_id {crawl_query_id}")

        try:
            # Create index with mapping if it doesn’t exist
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, mappings={
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

            # Prepare document for indexing
            doc = {
                "url": structured_data["url"],
                "title": structured_data["title"],
                "content": content_text,
                "metadata": structured_data["metadata"]
            }

            # Remove publish_date if it's empty to avoid parsing errors
            if not doc["metadata"]["publish_date"]:
                del doc["metadata"]["publish_date"]

            # Index in Elasticsearch with URL as _id
            es.index(index=index_name, id=doc["url"], document=doc)
            logging.info(f"Indexed content for {url} in {index_name}")

            # Take snapshot with retry loop for repository errors
            snapshot_name = f"snapshot_{crawl_query_id}_{uuid.uuid4().hex}"
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    response = es.snapshot.create(
                        repository=snapshot_repository,
                        snapshot=snapshot_name,
                        body={"indices": index_name},
                        wait_for_completion=True
                    )
                    if response.get("snapshot", {}).get("state") == "SUCCESS":
                        logging.info(f"Created snapshot {snapshot_name} for {index_name} in GCS")
                        break
                    else:
                        logging.error(f"Failed to create snapshot {snapshot_name}: {response}")
                        raise Exception("Snapshot creation not successful")
                except TransportError as e:
                    if 'repository_exception' in str(e) and attempt < max_retries - 1:
                        recover_repository()
                        continue
                    else:
                        raise
            else:
                raise Exception(f"Failed to create snapshot after {max_retries} attempts")

            message.ack()
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}")
            message.nack()

        # Enforce rate limiting: wait until at least RATE_LIMIT_DELAY has elapsed since the start
        elapsed_time = time.time() - start_time
        if elapsed_time < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed_time)

    # Subscribe to indexing tasks
    streaming_pull_future = subscriber.subscribe(indexing_subscription_path, callback=callback)
    logging.info(f"Listening for indexing tasks on {indexing_subscription_path}...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == '__main__':
    indexer_process()
