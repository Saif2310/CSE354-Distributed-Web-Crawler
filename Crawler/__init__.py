import json
import logging
import traceback
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor, defer
from twisted.internet.task import LoopingCall
from urllib.parse import urlparse
from datetime import datetime

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

# Global sets to track crawled URLs and their depths
crawled_urls = set()  # To prevent cyclic crawling
url_depths = {}  # To track the depth of each URL

def clean_content(response):
    """Extract structured data from the webpage response and return it as a JSON object."""
    # Extract title (from <title> tag or first <h1>)
    title = response.css('title::text').get() or response.css('h1::text').get() or "No Title"

    # Extract main content from a broader set of tags, including <span>
    # Use ::text to get text content directly, and exclude unwanted tags like script/style
    text_blocks = response.css(
        'p::text, div::text, article::text, span::text, '
        'blockquote::text, q::text, cite::text'
    ).getall()
    text_blocks = [text.strip() for text in text_blocks if text.strip()]  # Remove empty or whitespace-only strings

    # For quotes.toscrape.com specifically, prioritize quotes in <span class="text">
    # This can be generalized for other sites by targeting common classes for main content
    main_content_blocks = response.css(
        'span.text::text, '
        '.content::text, .main-content::text, .article-body::text, '
        '[role="main"] ::text, [itemprop="articleBody"] ::text'
    ).getall()
    main_content_blocks = [text.strip() for text in main_content_blocks if text.strip()]

    # If we found specific main content (like quotes), prioritize it; otherwise, fall back to general text blocks
    if main_content_blocks:
        text_blocks = main_content_blocks + text_blocks  # Prioritize main content, but include others as fallback

    # Deduplicate while preserving order (in case of overlapping selectors)
    seen = set()
    text_blocks = [text for text in text_blocks if not (text in seen or seen.add(text))]

    # Extract headings
    headings = {
        "h1": [h.strip() for h in response.css('h1::text').getall() if h.strip()],
        "h2": [h.strip() for h in response.css('h2::text').getall() if h.strip()],
        "h3": [h.strip() for h in response.css('h3::text').getall() if h.strip()],
    }

    # Extract links
    links = []
    for link in response.css('a'):
        href = link.css('::attr(href)').get()
        if href:
            absolute_url = response.urljoin(href)
            if is_valid_url(absolute_url):
                links.append({
                    "url": absolute_url,
                    "text": link.css('::text').get(default="").strip(),
                    "rel": link.css('::attr(rel)').get(default="")
                })

    # Extract metadata
    metadata = {
        "description": response.css('meta[name="description"]::attr(content)').get(default=""),
        "author": response.css('meta[name="author"]::attr(content)').get(default=""),
        "publish_date": response.css('meta[name="publish_date"]::attr(content)').get(default=""),
        "language": response.css('html::attr(lang)').get(default="en").split("-")[0]
    }
    keywords = response.css('meta[name="keywords"]::attr(content)').get(default="").split(",")
    metadata["keywords"] = [kw.strip() for kw in keywords if kw.strip()]

    # Extract source (e.g., from footer or meta)
    source_name = response.css('footer a::text').get(default="") or urlparse(response.url).netloc
    source_url = response.css('footer a::attr(href)').get(default="") or f"{urlparse(response.url).scheme}://{urlparse(response.url).netloc}/"
    
    # Infer content type (basic heuristic)
    content_type = "article"
    if "product" in response.url.lower() or response.css('.product-price, .add-to-cart').get():
        content_type = "product"
    elif "blog" in response.url.lower() or response.css('.blog-post, .post-meta').get():
        content_type = "blog"

    # Construct the structured JSON
    structured_data = {
        "url": response.url,
        "title": title,
        "main_content": {
            "text_blocks": text_blocks,
            "headings": headings
        },
        "links": links,
        "metadata": metadata,
        "source": {
            "name": source_name,
            "url": source_url
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "content_type": content_type,
        "extra_data": {}
    }

    return structured_data

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, current_depth, max_depth, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
        self.current_depth = current_depth
        self.max_depth = max_depth
    
    def parse(self, response):
        logging.info(f"Crawling {response.url} at depth {self.current_depth}")

        # Extract and clean content into structured JSON
        structured_data = clean_content(response)
        
        # Convert structured data to JSON string and upload to GCS
        content = json.dumps(structured_data, ensure_ascii=False).encode('utf-8')
        blob = bucket.blob(f"crawled/{response.url.replace('/', '_')}.json")
        blob.upload_from_string(content, content_type='application/json')
        logging.info(f"Stored structured content for {response.url} in GCS")

        # Publish indexing task
        indexing_task = {
            "url": response.url,
            "gcs_path": f"gs://{bucket_name}/crawled/{response.url.replace('/', '_')}.json"
        }
        publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
        logging.info(f"Published indexing task for {response.url}")

        # Extract new URLs if we're not at max depth
        if self.current_depth < self.max_depth:
            # Extract all href attributes
            hrefs = response.css('a::attr(href)').getall()
            # Convert all hrefs to absolute URLs using urljoin
            new_urls = [response.urljoin(href) for href in hrefs]
            # Filter out invalid URLs and ensure they are HTTP/HTTPS
            new_urls = [url for url in new_urls if is_valid_url(url)]

            next_depth = self.current_depth + 1
            for url in new_urls:
                if url not in crawled_urls:
                    url_depths[url] = next_depth
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": self.max_depth,
                        "current_depth": next_depth
                    }).encode('utf-8'))
                    logging.info(f"Published new URL {url} at depth {next_depth} to crawl queue")
            logging.info(f"Published {len(new_urls)} new URLs at depth {next_depth} to crawl queue")

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def crawl_url(url_to_crawl, current_depth, max_depth, callback):
    """Run Scrapy crawling within the Twisted reactor."""
    logging.info(f"Starting crawl for {url_to_crawl} at depth {current_depth}")
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
        deferred = process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl, current_depth=current_depth, max_depth=max_depth)
        deferred.addBoth(lambda _: process.stop())
        # Do not call process.start() here; rely on the reactor already running
        logging.info(f"Completed crawling {url_to_crawl} at depth {current_depth}")
        callback(True)
    except Exception as e:
        logging.error(f"Error crawling {url_to_crawl}: {str(e)}\n{traceback.format_exc()}")
        callback(False)

def callback_wrapper(success, ack_id, subscription_path):
    if success:
        subscriber.acknowledge(request={
            "subscription": subscription_path,
            "ack_ids": [ack_id],
        })
        logging.info(f"Acknowledged message for URL")
    else:
        subscriber.modify_ack_deadline(request={
            "subscription": subscription_path,
            "ack_ids": [ack_id],
            "ack_deadline_seconds": 0,
        })
        logging.info(f"Failed to crawl, nack message for retry")

def crawler_process():
    logging.info("Crawler node started")
    logging.info(f"Listening for crawl tasks on {crawl_subscription_path}...")

    def process_message():
        response = subscriber.pull(request={
            "subscription": crawl_subscription_path,
            "max_messages": 1,
        }, timeout=60)

        if not response.received_messages:
            logging.info("No messages received, waiting...")
            if not url_depths:
                crawled_urls.clear()
                logging.info("No pending URLs to crawl, reset crawled_urls set.")
            return

        for message in response.received_messages:
            try:
                task = json.loads(message.message.data.decode('utf-8'))
                url_to_crawl = task["url"]
                max_depth = task["max_depth"]
                current_depth = task.get("current_depth", 1)
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
                url_depths.pop(url_to_crawl, None)
                continue

            if current_depth > max_depth:
                logging.info(f"Depth {current_depth} exceeds max depth {max_depth} for {url_to_crawl}, skipping...")
                subscriber.acknowledge(request={
                    "subscription": crawl_subscription_path,
                    "ack_ids": [message.ack_id],
                })
                url_depths.pop(url_to_crawl, None)
                continue

            crawl_url(url_to_crawl, current_depth, max_depth, lambda success: callback_wrapper(success, message.ack_id, crawl_subscription_path))
            crawled_urls.add(url_to_crawl)

    # Use LoopingCall to periodically check for new messages
    lc = LoopingCall(process_message)
    lc.start(1.0)  # Check every 1 second

    # Start the Twisted reactor
    reactor.run()

if __name__ == '__main__':
    crawler_process()
