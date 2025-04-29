import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerProcess

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
bucket_name = "cse354-project-storage"
bucket = storage_client.bucket(bucket_name)

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
    
    def parse(self, response):
        logging.info(f"Crawling {response.url}")
        
        # Extract content (text)
        content = response.css('body').get()  # Extract body content
        if content:
            content = content.encode('utf-8')  # Convert to bytes for GCS
            
            # Store content in GCS
            blob = bucket.blob(f"crawled/{response.url.replace('/', '_')}.html")
            blob.upload_from_string(content)
            logging.info(f"Stored content for {response.url} in GCS")
            
            # Publish indexing task
            indexing_task = {
                "url": response.url,
                "gcs_path": f"gs://{bucket_name}/crawled/{response.url.replace('/', '_')}.html"
            }
            publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
            logging.info(f"Published indexing task for {response.url}")
        
        # Extract new URLs
        new_urls = response.css('a::attr(href)').getall()
        new_urls = [response.urljoin(url) for url in new_urls if url.startswith('http')]
        
        # Publish new URLs back to crawl queue
        for url in new_urls:
            publisher.publish(crawl_topic_path, url.encode('utf-8'))
        logging.info(f"Published {len(new_urls)} new URLs to crawl queue")

def crawler_process():
    logging.info("Crawler node started")

    def callback(message):
        url_to_crawl = message.data.decode('utf-8')
        logging.info(f"Received URL to crawl: {url_to_crawl}")
        
        try:
            # Start Scrapy crawler
            process = CrawlerProcess(settings={
                'DOWNLOAD_DELAY': 2,  # Politeness delay
                'ROBOTSTXT_OBEY': True,
                'LOG_LEVEL': 'INFO',
            })
            process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl)
            process.start()
            
            message.ack()
            logging.info(f"Completed crawling {url_to_crawl}")
        except Exception as e:
            logging.error(f"Error crawling {url_to_crawl}: {e}")
            message.nack()

    # Subscribe to crawl tasks
    streaming_pull_future = subscriber.subscribe(crawl_subscription_path, callback=callback)
    logging.info(f"Listening for crawl tasks on {crawl_subscription_path}...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == '__main__':
    crawler_process()
