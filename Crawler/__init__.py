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

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
    
    def parse(self, response):
        logging.info(f"Crawling {response.url}")
        
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
        
        new_urls = response.css('a::attr(href)').getall()
        new_urls = [response.urljoin(url) for url in new_urls if url.startswith('http')]
        
        for url in new_urls:
            publisher.publish(crawl_topic_path, url.encode('utf-8'))
        logging.info(f"Published {len(new_urls)} new URLs to crawl queue")

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def crawl_url(url_to_crawl):
    """Run Scrapy crawling in the main thread."""
    logging.info(f"Starting crawl for {url_to_crawl}")
    process = CrawlerProcess(settings={
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': True,
        'LOG_LEVEL': 'INFO',
        'TELNETCONSOLE_ENABLED': False,  # Disable TelnetConsole
    })
    process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl)
    process.start()
    logging.info(f"Completed crawling {url_to_crawl}")

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
            continue
        
        for message in response.received_messages:
            url_to_crawl = message.message.data.decode('utf-8')
            logging.info(f"Received URL to crawl: {url_to_crawl}")
            
            if not is_valid_url(url_to_crawl):
                logging.error(f"Invalid URL: {url_to_crawl}")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                continue
            
            try:
                # Crawl in the main thread
                crawl_url(url_to_crawl)
                
                # Acknowledge the message
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                logging.info(f"Acknowledged message for {url_to_crawl}")
            except Exception as e:
                logging.error(f"Error crawling {url_to_crawl}: {e}")
                # Nack the message to retry
                subscriber.modify_ack_deadline(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                    "ack_deadline_seconds": 0,  # This effectively nacks the message
                })

if __name__ == '__main__':
    crawler_process()
