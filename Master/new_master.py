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

    # Seed URLs
    seed_urls = ["http://example.com", "http://example.org"]

    # Publish seed URLs to crawl queue
    for url in seed_urls:
        publisher.publish(crawl_topic_path, url.encode('utf-8'))
        logging.info(f"Published URL to crawl: {url}")

    # In a real system, the master might listen for completion signals or monitor progress
    logging.info("Master node finished publishing seed URLs")

if __name__ == '__main__':
    master_process()