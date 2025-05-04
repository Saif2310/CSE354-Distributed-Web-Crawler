import json
import logging
from elasticsearch import Elasticsearch
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber import futures

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Search - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
search_subscription_name = "search-tasks-sub"
search_results_topic_name = "search-results-topic"
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
search_subscription_path = subscriber.subscription_path(project_id, search_subscription_name)
search_results_topic_path = publisher.topic_path(project_id, search_results_topic_name)

# Elasticsearch setup
es = Elasticsearch(["http://localhost:9200"])
snapshot_repository = "gcs_snapshots"

def setup_snapshot_repository():
    """Configure GCS as snapshot repository."""
    repo_body = {
        "type": "gcs",
        "settings": {
            "bucket": "cse354-project-storage",
            "base_path": "snapshots"
        }
    }
    es.snapshot.create_repository(repository=snapshot_repository, body=repo_body)
    logging.info("Configured GCS snapshot repository")

def get_snapshots_for_crawl_query(crawl_query_id):
    """Get all snapshots for the given crawl_query_id."""
    index_name = f"webpages_{crawl_query_id}"
    try:
        snapshots = es.snapshot.get(repository=snapshot_repository, snapshot="_all")["snapshots"]
        return [s["snapshot"] for s in snapshots if index_name in s["indices"]]
    except Exception as e:
        logging.error(f"Error retrieving snapshots: {e}")
        return []

def merge_indices(crawl_query_id, temp_indices):
    """Merge multiple temporary indices into a single index."""
    final_index = f"webpages_{crawl_query_id}"
    
    # Create the final index if it doesn't exist
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
    
    # Reindex all temporary indices into the final index
    for temp_index in temp_indices:
        reindex_body = {
            "source": {"index": temp_index},
            "dest": {"index": final_index}
        }
        es.reindex(body=reindex_body)
        logging.info(f"Reindexed {temp_index} into {final_index}")
    
    # Delete temporary indices
    for temp_index in temp_indices:
        es.indices.delete(index=temp_index, ignore=[404])
        logging.info(f"Deleted temporary index {temp_index}")
    
    return final_index

def search_query(crawl_query_id, query_text):
    """Search the index for a given crawl_query_id."""
    setup_snapshot_repository()
    snapshot_names = get_snapshots_for_crawl_query(crawl_query_id)
    
    if not snapshot_names:
        logging.error(f"No snapshots found for crawl_query_id {crawl_query_id}")
        return []

    # Restore each snapshot into a temporary index
    temp_indices = []
    for i, snapshot_name in enumerate(snapshot_names):
        temp_index = f"temp_{crawl_query_id}_{i}"
        es.snapshot.restore(
            repository=snapshot_repository,
            snapshot=snapshot_name,
            body={"indices": f"webpages_{crawl_query_id}", "rename_pattern": "webpages_(.+)", "rename_replacement": temp_index}
        )
        temp_indices.append(temp_index)
        logging.info(f"Restored snapshot {snapshot_name} into {temp_index}")

    # Merge temporary indices into a single index
    final_index = merge_indices(crawl_query_id, temp_indices)

    # Execute search query
    search_body = {
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
        result_message = {
            "crawl_query_id": crawl_query_id,
            "results": results
        }
        publisher.publish(search_results_topic_path, json.dumps(result_message).encode('utf-8'))
        logging.info(f"Published search results for crawl_query_id {crawl_query_id}")
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
