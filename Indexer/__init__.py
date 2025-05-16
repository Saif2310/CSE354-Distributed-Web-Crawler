import json
import logging
import time
import uuid
import threading
from google.cloud import pubsub_v1
from google.cloud import storage
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from urllib.request import Request, urlopen

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
indexing_subscription_name = "indexing-tasks-topic-sub"
updates_topic_name = "updates-subscription"
health_check_topic_name = "health-check"
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
indexing_subscription_path = subscriber.subscription_path(project_id, indexing_subscription_name)
updates_topic_path = publisher.topic_path(project_id, updates_topic_name)
health_check_topic_path = publisher.topic_path(project_id, health_check_topic_name)

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = "cse354-project-storage"
bucket = storage_client.bucket(bucket_name)

# Elasticsearch setup (local instance)
es = Elasticsearch(["http://localhost:9200"], timeout=30, max_retries=3, retry_on_timeout=True)
snapshot_repository = "gcs_snapshots"

# Rate limiting delay in seconds (202.08 ms = 0.20208 seconds)
RATE_LIMIT_DELAY = 0.20208

def get_machine_id():
    """Fetch the machine ID from GCP metadata."""
    try:
        req = Request('http://metadata.google.internal/computeMetadata/v1/instance/id', headers={'Metadata-Flavor': 'Google'})
        response = urlopen(req)
        return response.read().decode('utf-8')
    except Exception as e:
        logging.error(f"Failed to get machine ID: {e}")
        return "unknown"

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

def run_heartbeat_in_thread(machine_id):
    """Run periodic heartbeats in a separate thread without a Twisted reactor."""
    def send_heartbeat():
        heartbeat_message = json.dumps({
            "type": "heartbeat",
            "machine_type": "Indexer",
            "machine_id": machine_id
        })
        publisher.publish(health_check_topic_path, heartbeat_message.encode('utf-8'))
        logging.info(f"Published heartbeat: {heartbeat_message}")

    while True:
        send_heartbeat()
        time.sleep(10.0)

def indexer_process():
    logging.info("Indexer node started")
    setup_snapshot_repository()
    machine_id = get_machine_id()
    processed_crawl_ids = set()
    last_message_times = {}  # Track last message time per crawl_query_id

    # Start heartbeat in a separate thread
    heartbeat_thread = threading.Thread(target=run_heartbeat_in_thread, args=(machine_id,), daemon=True)
    heartbeat_thread.start()

    def check_completion():
        current_time = time.time()
        for crawl_query_id, last_time in list(last_message_times.items()):
            if current_time - last_time > 30:  # Timeout after 30 seconds
                index_name = f"webpages_{crawl_query_id}"
                snapshot_name = f"snapshot_{crawl_query_id}_{uuid.uuid4().hex}"
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        response = es.snapshot.create(
                            repository=snapshot_repository,
                            snapshot=snapshot_name,
                            body={"indices": index_name},
                            wait_for_completion=False
                        )
                        logging.info(f"Initiated snapshot {snapshot_name} for {index_name} in GCS")
                        break
                    except TransportError as e:
                        if 'repository_exception' in str(e) and attempt < max_retries - 1:
                            recover_repository()
                            continue
                        else:
                            raise
                else:
                    logging.error(f"Failed to initiate snapshot {snapshot_name} after {max_retries} attempts")
                    # Continue to publish completion even if snapshot fails
                completion_message = {"index_complete": True, "crawl_query_id": crawl_query_id}
                publisher.publish(updates_topic_path, json.dumps(completion_message).encode('utf-8'))
                logging.info(f"Published index completion for {crawl_query_id}: {completion_message}")
                del last_message_times[crawl_query_id]

    completion_lc = LoopingCall(check_completion)
    completion_lc.start(1.0)

    def callback(message):
        start_time = time.time()
        
        task = json.loads(message.data.decode('utf-8'))
        url = task["url"]
        gcs_path = task["gcs_path"]
        crawl_query_id = task["crawl_query_id"]
        index_name = f"webpages_{crawl_query_id}"
        logging.info(f"Received indexing task for {url} with crawl_query_id {crawl_query_id}")

        try:
            last_message_times[crawl_query_id] = time.time()

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

            blob_name = gcs_path.replace(f"gs://{bucket_name}/", "")
            blob = bucket.blob(blob_name)
            content = blob.download_as_text()
            structured_data = json.loads(content)

            headings = structured_data["main_content"]["headings"]
            heading_text = " ".join(
                headings.get("h1", []) +
                headings.get("h2", []) +
                headings.get("h3", [])
            )
            text_blocks = " ".join(structured_data["main_content"]["text_blocks"])
            content_text = heading_text + " " + text_blocks

            doc = {
                "url": structured_data["url"],
                "title": structured_data["title"],
                "content": content_text,
                "metadata": structured_data["metadata"]
            }

            if not doc["metadata"]["publish_date"]:
                del doc["metadata"]["publish_date"]

            es.index(index=index_name, id=doc["url"], document=doc)
            logging.info(f"Indexed content for {url} in {index_name}")

            index_message = {"indexed_count": 1, "crawl_query_id": crawl_query_id}
            publisher.publish(updates_topic_path, json.dumps(index_message).encode('utf-8'))
            logging.info(f"Published indexed notification: {index_message}")

            message.ack()
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}")
            message.nack()

        elapsed_time = time.time() - start_time
        if elapsed_time < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed_time)

    streaming_pull_future = subscriber.subscribe(indexing_subscription_path, callback=callback)
    logging.info(f"Listening for indexing tasks on {indexing_subscription_path}...")
    
    try:
        streaming_pull_future.result()
        reactor.run()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        reactor.stop()

if __name__ == '__main__':
    indexer_process()
