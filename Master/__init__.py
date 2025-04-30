import json
import logging
from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
crawl_topic_name = "crawl-tasks-topic"
publisher = pubsub_v1.PublisherClient()
crawl_topic_path = publisher.topic_path(project_id, crawl_topic_name)

def master_process():
    logging.info("Master node started")

    # Seed tasks with URLs and max_depth
    seed_tasks = [
        {"url": "http://example.com", "max_depth": 3},
        {"url": "http://example.org", "max_depth": 2}
    ]

    # Publish seed tasks to crawl queue
    for task in seed_tasks:
        task_json = json.dumps(task)
        publisher.publish(crawl_topic_path, task_json.encode('utf-8'))
        logging.info(f"Published task to crawl: {task_json}")

    # In a real system, the master might listen for completion signals or monitor progress
    logging.info("Master node finished publishing seed tasks")

if __name__ == '__main__':
    master_process()