import json
import logging
from elasticsearch import Elasticsearch
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber import futures
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Search - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
search_subscription_name = "search-tasks-topic-sub"
search_results_topic_name = "search-results-topic"
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
search_subscription_path = subscriber.subscription_path(project_id, search_subscription_name)
search_results_topic_path = publisher.topic_path(project_id, search_results_topic_name)

# Elasticsearch setup
es = Elasticsearch(["http://localhost:9200"])
snapshot_repository = "local_gcs_snapshots"
gcs_snapshot_repository = "gcs_snapshots"
local_snapshot_path = "/local/snapshots"  # Adjust this path as needed
gcs_snapshot_path = "gs://cse354-project-storage/snapshots"

def sync_gcs_to_local():
    """Sync the GCS snapshots directory to the local filesystem."""
    try:
        logging.info(f"Syncing GCS {gcs_snapshot_path} to local {local_snapshot_path}")
        result = subprocess.run(
            ["gsutil", "rsync", "-r", gcs_snapshot_path, local_snapshot_path],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"Sync completed successfully: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to sync GCS to local: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during sync: {str(e)}")
        raise

def setup_snapshot_repository():
    """Configure local filesystem and GCS as snapshot repositories."""
    # Configure local filesystem repository
    local_repo_body = {
        "type": "fs",
        "settings": {
            "location": local_snapshot_path,
            "readonly": True
        }
    }
    try:
        es.snapshot.get_repository(repository=snapshot_repository)
        logging.info(f"Snapshot repository {snapshot_repository} already exists, deleting it")
        es.snapshot.delete_repository(repository=snapshot_repository, ignore=[404])
        logging.info(f"Deleted snapshot repository {snapshot_repository}")
    except Exception:
        logging.info(f"Snapshot repository {snapshot_repository} does not exist")
    es.snapshot.create_repository(repository=snapshot_repository, body=local_repo_body)
    logging.info(f"Created local filesystem snapshot repository {snapshot_repository}")

    # Configure GCS repository as fallback
    gcs_repo_body = {
        "type": "gcs",
        "settings": {
            "bucket": "cse354-project-storage",
            "base_path": "snapshots"
        }
    }
    try:
        es.snapshot.get_repository(repository=gcs_snapshot_repository)
        logging.info(f"Snapshot repository {gcs_snapshot_repository} already exists, deleting it")
        es.snapshot.delete_repository(repository=gcs_snapshot_repository, ignore=[404])
        logging.info(f"Deleted snapshot repository {gcs_snapshot_repository}")
    except Exception:
        logging.info(f"Snapshot repository {gcs_snapshot_repository} does not exist")
    es.snapshot.create_repository(repository=gcs_snapshot_repository, body=gcs_repo_body)
    logging.info(f"Created GCS snapshot repository {gcs_snapshot_repository}")

    # Verify both repositories
    try:
        es.snapshot.verify_repository(repository=snapshot_repository)
        logging.info(f"Verified local snapshot repository {snapshot_repository}")
    except Exception as e:
        logging.error(f"Failed to verify local snapshot repository {snapshot_repository}: {e}")
        raise
    try:
        es.snapshot.verify_repository(repository=gcs_snapshot_repository)
        logging.info(f"Verified GCS snapshot repository {gcs_snapshot_repository}")
    except Exception as e:
        logging.error(f"Failed to verify GCS snapshot repository {gcs_snapshot_repository}: {e}")
        raise

def get_snapshots_for_crawl_query(crawl_query_id):
    """Get all snapshots for the given crawl_query_id, preferring local repository."""
    index_name = f"webpages_{crawl_query_id}"
    try:
        # Try local repository first
        snapshots = es.snapshot.get(repository=snapshot_repository, snapshot="_all")["snapshots"]
        logging.info(f"Available snapshots from local: {[s['snapshot'] for s in snapshots]}")
        relevant_snapshots = [s["snapshot"] for s in snapshots if any(index_name in idx for idx in s.get("indices", []))]
        if relevant_snapshots:
            return relevant_snapshots

        # Fall back to GCS repository if no snapshots found locally
        snapshots = es.snapshot.get(repository=gcs_snapshot_repository, snapshot="_all")["snapshots"]
        logging.info(f"Available snapshots from GCS: {[s['snapshot'] for s in snapshots]}")
        relevant_snapshots = [s["snapshot"] for s in snapshots if any(index_name in idx for idx in s.get("indices", []))]
        if not relevant_snapshots:
            logging.warning(f"No snapshots found for crawl_query_id {crawl_query_id}, checking all indices")
            relevant_snapshots = [s["snapshot"] for s in snapshots]
        return relevant_snapshots
    except Exception as e:
        logging.error(f"Error retrieving snapshots: {e}")
        return []

def merge_indices(crawl_query_id, temp_indices):
    """Merge multiple temporary indices into a single index with deduplication."""
    final_index = f"webpages_{crawl_query_id}"
    
    if not es.indices.exists(index=final_index):
        es.indices.create(index=final_index, body={
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
    
    for temp_index in temp_indices:
        reindex_body = {
            "source": {"index": temp_index},
            "dest": {"index": final_index, "op_type": "create"},
            "script": {"source": "ctx._id = ctx._source.url", "lang": "painless"},
            "conflicts": "proceed"
        }
        response = es.reindex(body=reindex_body)
        logging.info(f"Reindex response for {temp_index}: {response}")
        logging.info(f"Reindexed {temp_index} into {final_index}")
    
    # Refresh the index to make documents searchable
    es.indices.refresh(index=final_index)
    logging.info(f"Refreshed index {final_index} for search")

    for temp_index in temp_indices:
        es.indices.delete(index=temp_index, ignore=[404])
        logging.info(f"Deleted temporary index {temp_index}")
    
    return final_index

def restore_snapshot(snapshot_name, crawl_query_id):
    """Restore a single snapshot into a temporary index, using local repository first."""
    temp_index = f"temp_{crawl_query_id}_{uuid.uuid4().hex[:8]}"
    if es.indices.exists(index=temp_index):
        es.indices.delete(index=temp_index)
        logging.info(f"Deleted existing temporary index {temp_index}")
    # Try local repository first
    try:
        es.snapshot.restore(
            repository=snapshot_repository,
            snapshot=snapshot_name,
            body={"indices": f"webpages_{crawl_query_id}", "rename_pattern": "webpages_(.+)", "rename_replacement": temp_index},
            wait_for_completion=True
        )
        logging.info(f"Restored snapshot {snapshot_name} from local into {temp_index}")
        return temp_index
    except Exception as e:
        logging.warning(f"Failed to restore {snapshot_name} from local: {e}")
        # Fall back to GCS repository
        es.snapshot.restore(
            repository=gcs_snapshot_repository,
            snapshot=snapshot_name,
            body={"indices": f"webpages_{crawl_query_id}", "rename_pattern": "webpages_(.+)", "rename_replacement": temp_index},
            wait_for_completion=True
        )
        logging.info(f"Restored snapshot {snapshot_name} from GCS into {temp_index}")
        return temp_index

def search_query(crawl_query_id, query_text):
    """Search the index for a given crawl_query_id with parallel restores."""
    setup_snapshot_repository()
    
    # Sync GCS snapshots to local directory before proceeding
    sync_gcs_to_local()
    
    snapshot_names = get_snapshots_for_crawl_query(crawl_query_id)
    
    if not snapshot_names:
        logging.error(f"No snapshots found for crawl_query_id {crawl_query_id}")
        return []

    # Parallel restore of snapshots
    temp_indices = []
    max_concurrent_restores = 4  # Adjust based on your system's capacity
    with ThreadPoolExecutor(max_workers=max_concurrent_restores) as executor:
        future_to_snapshot = {executor.submit(restore_snapshot, snapshot_name, crawl_query_id): snapshot_name for snapshot_name in snapshot_names}
        for future in as_completed(future_to_snapshot):
            try:
                temp_index = future.result()
                temp_indices.append(temp_index)
            except Exception as e:
                logging.error(f"Error restoring snapshot {future_to_snapshot[future]}: {e}")

    final_index = merge_indices(crawl_query_id, temp_indices)

    # Log the number of documents in the index for debugging
    count = es.count(index=final_index)["count"]
    logging.info(f"Number of documents in {final_index}: {count}")

    search_body = {
        "size": 5,
        "query": {
            "multi_match": {
                "query": query_text,
                "fields": ["title^2", "content"]
            }
        }
    }
    results = es.search(index=final_index, body=search_body)
    hits = results["hits"]["hits"]
    return [{"url": hit["_source"]["url"], "title": hit["_source"]["title"], "score": hit["_score"]} for hit in hits]

def callback(message):
    """Handle search tasks from Pub/Sub."""
    task = json.loads(message.data.decode('utf-8'))
    crawl_query_id = task["crawl_query_id"]
    query_text = task["query_text"]
    logging.info(f"Received search task for crawl_query_id {crawl_query_id}: {query_text}")

    try:
        results = search_query(crawl_query_id, query_text)
        print("Search Results:", json.dumps(results, indent=2))
        result_message = {"crawl_query_id": crawl_query_id, "results": results}
        publisher.publish(search_results_topic_path, json.dumps(result_message).encode('utf-8'))
        logging.info(f"Published search results for crawl_query_id {crawl_query_id}")

        # Clear all restored and created indices (webpages_* and temp_*), preserving system indices
        try:
            es.indices.delete(index=["webpages_*", "temp_*"], ignore=[404])
            logging.info("Cleared all restored and created indices (webpages_* and temp_*)")
        except Exception as e:
            logging.error(f"Error clearing indices webpages_* and temp_*: {e}")

        message.ack()
    except Exception as e:
        logging.error(f"Error processing search task: {e}")
        message.nack()

def search_service_process():
    logging.info("Search service started")
    streaming_pull_future = subscriber.subscribe(search_subscription_path, callback=callback)
    logging.info(f"Listening for search tasks on {search_subscription_path}...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == '__main__':
    search_service_process()
