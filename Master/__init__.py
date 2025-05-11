import json
import logging
import uuid
from google.cloud import pubsub_v1
from flask import Flask, request, jsonify
import threading
import asyncio
import websockets

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Pub/Sub setup
project_id = "glass-episode-457618-i2"
crawl_topic_name = "crawl-tasks-topic"
search_topic_name = "search-tasks-topic"
publisher = pubsub_v1.PublisherClient()
crawl_topic_path = publisher.topic_path(project_id, crawl_topic_name)
search_topic_path = publisher.topic_path(project_id, search_topic_name)

# Flask app for HTTP endpoints
app = Flask(__name__)

# WebSocket setup
connected_clients = set()

async def websocket_handler(websocket, path):
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

start_server = websockets.serve(websocket_handler, "0.0.0.0", 8765)

# Pub/Sub subscriber for updates
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, "updates-subscription")

def callback(message):
    data = json.loads(message.data.decode('utf-8'))
    message.ack()
    # Broadcast updates to WebSocket clients
    message_data = {}
    if 'crawled_count' in data:
        message_data = {'type': 'crawled', 'count': data['crawled_count']}
    elif 'indexed_count' in data:
        message_data = {'type': 'indexed', 'count': data['indexed_count']}
    elif 'crawl_complete' in data:
        message_data = {'type': 'crawl_complete'}
    elif 'index_complete' in data:
        message_data = {'type': 'index_complete'}
    elif 'search_progress' in data:
        message_data = {'type': 'search_progress', 'processed': data['processed'], 'total': data['total']}
    elif 'search_results' in data:
        message_data = {'type': 'search_results', 'results': data['results']}
    
    if message_data:
        for client in connected_clients:
            asyncio.run(client.send(json.dumps(message_data)))

subscriber.subscribe(subscription_path, callback=callback)

# HTTP endpoint for crawl initiation
@app.route('/crawl', methods=['POST'])
def crawl():
    tasks = request.json
    crawl_uuid = str(uuid.uuid4())
    for task in tasks:
        task['current_depth'] = 1
        task['crawl_query_id'] = crawl_uuid
        task_json = json.dumps(task)
        publisher.publish(crawl_topic_path, task_json.encode('utf-8'))
        logging.info(f"Published task to crawl: {task_json}")
    return jsonify({"uuid": crawl_uuid})

# HTTP endpoint for search initiation
@app.route('/search', methods=['POST'])
def search():
    data = request.json
    search_task = {"query_text": data['keyword'], "crawl_query_id": data['uuid']}
    publisher.publish(search_topic_path, json.dumps(search_task).encode('utf-8'))
    logging.info(f"Published search task: {json.dumps(search_task)}")
    return jsonify({"status": "search initiated"})

# Dummy crawl tasks for testing
def dummy_master_process():
    logging.info("Dummy master node started")
    seed_tasks = [
        {"url": "http://example.com", "max_depth": 3, "current_depth": 1, "crawl_query_id": "test_uuid_1"},
        {"url": "http://example.org", "max_depth": 2, "current_depth": 1, "crawl_query_id": "test_uuid_1"}
    ]
    for task in seed_tasks:
        task_json = json.dumps(task)
        publisher.publish(crawl_topic_path, task_json.encode('utf-8'))
        logging.info(f"Published dummy task to crawl: {task_json}")
    logging.info("Dummy master node finished publishing seed tasks")

if __name__ == '__main__':
    # Start WebSocket server in a separate thread
    asyncio.get_event_loop().run_until_complete(start_server)
    threading.Thread(target=asyncio.get_event_loop().run_forever, daemon=True).start()
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)
    # Uncomment the following line to test with dummy tasks
    # dummy_master_process()
