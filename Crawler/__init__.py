import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
crawl_subscription_name = "crawl-tasks-topic-sub"
crawl_topic_name = "crawl-tasks-topic"
indexing_topic_name = "indexing-tasks-topic"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
crawl_subscription_path = subscriber.subscription_path(project_id, crawl_subscription_name)
crawl_topic_path = publisher.topic_path(project_id, crawl_topic_name)
indexing_topic_path = publisher.topic_path(project_id, indexing_topic_name)

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = "cse354-project-storage"  # Replace with your bucket name
bucket = storage_client.bucket(bucket_name)

# Global sets to track crawled URLs and their depths
crawled_urls = set()  # To prevent cyclic crawling
url_depths = {}  # To track the depth of each URL

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, current_depth, max_depth, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
        self.current_depth = current_depth
        self.max_depth = max_depth
    
    def parse(self, response):
        logging.info(f"Crawling {response.url} at depth {self.current_depth}")
        
        # Mark this URL as crawled
        crawled_urls.add(response.url)
        
        # Extract content (text)
        content = response.css('body').get()
        if content:
            content = content.encode('utf-8')
            blob = bucket.blob(f"crawled/{response.url.replace('/', '_')}.html")
            blob.upload_from_string(content)
            logging.info(f"Stored content for {response.url} in GCS")
            
            indexing_task = {
                "url": response.url,
                "gcs_path": f"gs://{bucket_name}/crawled/{response.url.replace('/', '_')}.html"
            }
            publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
            logging.info(f"Published indexing task for {response.url}")
        
        # Extract new URLs if we're not at max depth
        if self.current_depth < self.max_depth:
            new_urls = response.css('a::attr(href)').getall()
            new_urls = [response.urljoin(url) for url in new_urls if url.startswith('http')]
            
            next_depth = self.current_depth + 1
            for url in new_urls:
                if url not in crawled_urls:
                    url_depths[url] = next_depth
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": self.max_depth,
                        "current_depth": next_depth
                    }).encode('utf-8'))
            logging.info(f"Published {len(new_urls)} new URLs at depth {next_depth} to crawl queue")

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def crawl_url(url_to_crawl, current_depth, max_depth):
    """Run Scrapy crawling in the main thread."""
    logging.info(f"Starting crawl for {url_to_crawl} at depth {current_depth}")
    process = CrawlerProcess(settings={
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': True,
        'LOG_LEVEL': 'INFO',
        'TELNETCONSOLE_ENABLED': False,
    })
    process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl, current_depth=current_depth, max_depth=max_depth)
    process.start()
    logging.info(f"Completed crawling {url_to_crawl} at depth {current_depth}")

def crawler_process():
    logging.info("Crawler node started")
    logging.info(f"Listening for crawl tasks on {crawl_subscription_path}...")
    
    while True:
        # Pull messages synchronously in the main thread
        response = subscriber.pull(request={
            "subscription": crawl_subscription_path,
            "max_messages": 1,
        }, timeout=60)
        
        if not response.received_messages:
            logging.info("No messages received, waiting...")
            # Reset state if no messages are pending (i.e., crawling is complete)
            if not url_depths:
                crawled_urls.clear()
                logging.info("No pending URLs to crawl, reset crawled_urls set.")
            continue
        
        for message in response.received_messages:
            try:
                task = json.loads(message.message.data.decode('utf-8'))
                url_to_crawl = task["url"]
                max_depth = task["max_depth"]
                current_depth = task.get("current_depth", 1)  # Default to 1 for root URL
            except (json.JSONDecodeError, KeyError) as e:
                logging.error(f"Invalid message format: {e}")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                continue
            
            logging.info(f"Received URL to crawl: {url_to_crawl} at depth {current_depth} with max depth {max_depth}")
            
            if not is_valid_url(url_to_crawl):
                logging.error(f"Invalid URL: {url_to_crawl}")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                continue
            
            if url_to_crawl in crawled_urls:
                logging.info(f"URL already crawled: {url_to_crawl}, skipping...")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                # Remove from url_depths since we're done with this URL
                url_depths.pop(url_to_crawl, None)
                continue
            
            if current_depth > max_depth:
                logging.info(f"Depth {current_depth} exceeds max depth {max_depth} for {url_to_crawl}, skipping...")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                # Remove from url_depths since we're done with this URL
                url_depths.pop(url_to_crawl, None)
                continue
            
            try:
                # Crawl in the main thread
                crawl_url(url_to_crawl, current_depth, max_depth)
                
                # Acknowledge the message
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                logging.info(f"Acknowledged message for {url_to_crawl}")
                
                # Remove from url_depths since we're done with this URL
                url_depths.pop(url_to_crawl, None)
            except Exception as e:
                logging.error(f"Error crawling {url_to_crawl}: {e}")
                # Nack the message to retry
                subscriber.modify_ack_deadline(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                    "ack_deadline_seconds": 0,
                })

if __name__ == '__main__':
    crawler_process()
