import json
import logging
import traceback
import threading
import time
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor, defer
from twisted.internet.task import LoopingCall
from urllib.parse import urlparse
from urllib.request import Request, urlopen

# Configure a specific logger for Crawler
logger = logging.getLogger('Crawler')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - Crawler - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
crawl_subscription_name = "crawl-tasks-topic-sub"
crawl_topic_name = "crawl-tasks-topic"
indexing_topic_name = "indexing-tasks-topic"
updates_topic_name = "updates-subscription"
health_check_topic_name = "health-check"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
crawl_subscription_path = subscriber.subscription_path(project_id, crawl_subscription_name)
crawl_topic_path = publisher.topic_path(project_id, crawl_topic_name)
indexing_topic_path = publisher.topic_path(project_id, indexing_topic_name)
updates_topic_path = publisher.topic_path(project_id, updates_topic_name)
health_check_topic_path = publisher.topic_path(project_id, health_check_topic_name)

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = "cse354-project-storage"
bucket = storage_client.bucket(bucket_name)

# Global dictionaries to track crawled URLs and their depths per crawl_query_id
crawled_urls = {}  # Maps crawl_query_id to set of URLs
url_depths = {}    # Maps crawl_query_id to {url: depth}

def clean_content(response):
    """Extract structured data from the webpage response."""
    title = response.css('title::text').get() or response.css('h1::text').get() or "No Title"
    text_blocks = response.css(
        'p::text, div::text, article::text, span::text, '
        'blockquote::text, q::text, cite::text'
    ).getall()
    text_blocks = [text.strip() for text in text_blocks if text.strip()]
    main_content_blocks = response.css(
        'span.text::text, '
        '.content::text, .main-content::text, .article-body::text, '
        '[role="main"] ::text, [itemprop="articleBody"] ::text'
    ).getall()
    main_content_blocks = [text.strip() for text in main_content_blocks if text.strip()]
    if main_content_blocks:
        text_blocks = main_content_blocks + text_blocks
    seen = set()
    text_blocks = [text for text in text_blocks if not (text in seen or seen.add(text))]
    headings = {
        "h1": [h.strip() for h in response.css('h1::text').getall() if h.strip()],
        "h2": [h.strip() for h in response.css('h2::text').getall() if h.strip()],
        "h3": [h.strip() for h in response.css('h3::text').getall() if h.strip()],
    }
    metadata = {
        "description": response.css('meta[name="description"]::attr(content)').get(default=""),
        "author": response.css('meta[name="author"]::attr(content)').get(default=""),
        "publish_date": response.css('meta[name="publish_date"]::attr(content)').get(default=""),
        "language": response.css('html::attr(lang)').get(default="en").split("-")[0]
    }
    keywords = response.css('meta[name="keywords"]::attr(content)').get(default="").split(",")
    metadata["keywords"] = [kw.strip() for kw in keywords if kw.strip()]
    structured_data = {
        "url": response.url,
        "title": title,
        "main_content": {
            "text_blocks": text_blocks,
            "headings": headings
        },
        "metadata": metadata
    }
    return structured_data

def get_machine_id():
    """Fetch the machine ID from GCP metadata."""
    try:
        req = Request('http://metadata.google.internal/computeMetadata/v1/instance/id', headers={'Metadata-Flavor': 'Google'})
        response = urlopen(req)
        return response.read().decode('utf-8')
    except Exception as e:
        logger.error(f"Failed to get machine ID: {e}")
        return "unknown"

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, current_depth, max_depth, crawl_query_id, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
        self.current_depth = current_depth
        self.max_depth = max_depth
        self.crawl_query_id = crawl_query_id
    
    def parse(self, response):
        logger.info(f"Crawling {response.url} at depth {self.current_depth}")
        structured_data = clean_content(response)
        content = json.dumps(structured_data, ensure_ascii=False).encode('utf-8')
        blob = bucket.blob(f"crawled/{response.url.replace('/', '_')}.json")
        blob.upload_from_string(content, content_type='application/json')
        logger.info(f"Stored structured content for {response.url} in GCS")
        indexing_task = {
            "url": response.url,
            "gcs_path": f"gs://{bucket_name}/crawled/{response.url.replace('/', '_')}.json",
            "crawl_query_id": self.crawl_query_id
        }
        publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
        logger.info(f"Published indexing task for {response.url}")
        
        # Publish notification for crawled page
        crawl_message = {"crawled_count": 1, "crawl_query_id": self.crawl_query_id}
        publisher.publish(updates_topic_path, json.dumps(crawl_message).encode('utf-8'))
        logger.info(f"Published crawled notification: {crawl_message}")
        
        if self.current_depth < self.max_depth:
            hrefs = response.css('a::attr(href)').getall()
            new_urls = [response.urljoin(href) for href in hrefs]
            new_urls = [url for url in new_urls if is_valid_url(url)]
            next_depth = self.current_depth + 1
            for url in new_urls:
                if url not in crawled_urls.get(self.crawl_query_id, set()):
                    url_depths.setdefault(self.crawl_query_id, {})[url] = next_depth
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": self.max_depth,
                        "current_depth": next_depth,
                        "crawl_query_id": self.crawl_query_id
                    }).encode('utf-8'))
                    logger.info(f"Published new URL {url} at depth {next_depth} to crawl queue")
            logger.info(f"Published {len(new_urls)} new URLs at depth {next_depth} to crawl queue")

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def crawl_url(url_to_crawl, current_depth, max_depth, crawl_query_id, callback):
    """Run Scrapy crawling within the Twisted reactor."""
    logger.info(f"Starting crawl for {url_to_crawl} at depth {current_depth}")
    try:
        process = CrawlerProcess(settings={
            'DOWNLOAD_DELAY': 2,
            'ROBOTSTXT_OBEY': True,
            'LOG_LEVEL': 'INFO',
            'TELNETCONSOLE_ENABLED': False,
            'RETRY_ENABLED': True,
            'RETRY_TIMES': 3,
            'HTTPERROR_ALLOWED_CODES': [403, 429, 503],
        })
        deferred = process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl, current_depth=current_depth, max_depth=max_depth, crawl_query_id=crawl_query_id)
        deferred.addBoth(lambda _: process.stop())
        logger.info(f"Completed crawling {url_to_crawl} at depth {current_depth}")
        callback(True)
    except Exception as e:
        logger.error(f"Error crawling {url_to_crawl}: {str(e)}\n{traceback.format_exc()}")
        callback(False)

def callback_wrapper(success, ack_id, subscription_path):
    if success:
        subscriber.acknowledge(request={
            "subscription": subscription_path,
            "ack_ids": [ack_id],
        })
        logger.info(f"Acknowledged message for URL")
    else:
        subscriber.modify_ack_deadline(request={
            "subscription": subscription_path,
            "ack_ids": [ack_id],
            "ack_deadline_seconds": 0,
        })
        logger.info(f"Failed to crawl, nack message for retry")

def run_heartbeat_in_thread(machine_id):
    """Run periodic heartbeats in a separate thread without a Twisted reactor."""
    def send_heartbeat():
        heartbeat_message = json.dumps({
            "type": "heartbeat",
            "machine_type": "Crawler",
            "machine_id": machine_id
        })
        publisher.publish(health_check_topic_path, heartbeat_message.encode('utf-8'))
        logger.info(f"Published heartbeat: {heartbeat_message}")

    while True:
        send_heartbeat()
        time.sleep(10.0)

def crawler_process():
    global machine_id
    machine_id = get_machine_id()
    logger.info("Crawler node started")
    logger.info(f"Listening for crawl tasks on {crawl_subscription_path}...")
    
    # Start heartbeat in a separate thread
    heartbeat_thread = threading.Thread(target=run_heartbeat_in_thread, args=(machine_id,), daemon=True)
    heartbeat_thread.start()
    
    last_message_times = {}  # Track last message time per crawl_query_id
    
    def check_completion():
        current_time = time.time()
        for crawl_query_id, last_time in list(last_message_times.items()):
            if current_time - last_time > 30:  # Increased timeout to 30 seconds
                completion_message = {"crawl_complete": True, "crawl_query_id": crawl_query_id}
                publisher.publish(updates_topic_path, json.dumps(completion_message).encode('utf-8'))
                logger.info(f"Published crawl completion for {crawl_query_id}: {completion_message}")
                del last_message_times[crawl_query_id]
                crawled_urls.pop(crawl_query_id, None)  # Clear URLs for completed crawl
                url_depths.pop(crawl_query_id, None)    # Clear depths for completed crawl

    completion_lc = LoopingCall(check_completion)
    completion_lc.start(1.0)
    
    def process_message():
        response = subscriber.pull(request={
            "subscription": crawl_subscription_path,
            "max_messages": 1,
        }, timeout=60)

        if response.received_messages:
            for message in response.received_messages:
                try:
                    task = json.loads(message.message.data.decode('utf-8'))
                    url_to_crawl = task["url"]
                    max_depth = task["max_depth"]
                    current_depth = task.get("current_depth", 1)
                    crawl_query_id = task["crawl_query_id"]
                    last_message_times[crawl_query_id] = time.time()
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Invalid message format: {e}")
                    subscriber.acknowledge(request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                    })
                    continue

                logger.info(f"Received URL to crawl: {url_to_crawl} at depth {current_depth} with max depth {max_depth} and crawl_query_id {crawl_query_id}")

                if not is_valid_url(url_to_crawl):
                    logger.error(f"Invalid URL: {url_to_crawl}")
                    subscriber.acknowledge(request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                    })
                    continue

                # Initialize sets/dicts for this crawl_query_id
                crawled_urls.setdefault(crawl_query_id, set())
                url_depths.setdefault(crawl_query_id, {})

                # Check if URL was crawled for this crawl_query_id
                if url_to_crawl in crawled_urls[crawl_query_id]:
                    logger.info(f"URL already crawled for this crawl_query_id {crawl_query_id}: {url_to_crawl}, skipping...")
                    subscriber.acknowledge(request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                    })
                    url_depths[crawl_query_id].pop(url_to_crawl, None)
                    continue

                # Check if URL was crawled for another crawl_query_id
                other_crawl_ids = [cid for cid in crawled_urls if cid != crawl_query_id and url_to_crawl in crawled_urls[cid]]
                if other_crawl_ids:
                    logger.info(f"URL {url_to_crawl} already crawled for other crawl_query_id {other_crawl_ids}, publishing indexing task")
                    indexing_task = {
                        "url": url_to_crawl,
                        "gcs_path": f"gs://{bucket_name}/crawled/{url_to_crawl.replace('/', '_')}.json",
                        "crawl_query_id": crawl_query_id
                    }
                    publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
                    logger.info(f"Published indexing task for {url_to_crawl}")
                    crawl_message = {"crawled_count": 1, "crawl_query_id": crawl_query_id}
                    publisher.publish(updates_topic_path, json.dumps(crawl_message).encode('utf-8'))
                    logger.info(f"Published crawled notification: {crawl_message}")
                    crawled_urls[crawl_query_id].add(url_to_crawl)
                    subscriber.acknowledge(request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                    })
                    continue

                if current_depth > max_depth:
                    logger.info(f"Depth {current_depth} exceeds max depth {max_depth} for {url_to_crawl}, skipping...")
                    subscriber.acknowledge(request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                    })
                    url_depths[crawl_query_id].pop(url_to_crawl, None)
                    continue

                crawl_url(url_to_crawl, current_depth, max_depth, crawl_query_id, lambda success: callback_wrapper(success, message.ack_id, crawl_subscription_path))
                crawled_urls[crawl_query_id].add(url_to_crawl)

    lc = LoopingCall(process_message)
    lc.start(1.0)
    reactor.run()

if __name__ == '__main__':
    crawler_process()
