import json
import logging
import uuid
import time
from google.cloud import pubsub_v1
from flask import Flask, request, jsonify
import threading
import asyncio
import websockets
from queue import Queue, Empty  # Import Empty exception explicitly

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

project_id = "glass-episode-457618-i2"
crawl_topic_name = "crawl-tasks-topic"
search_topic_name = "search-tasks-topic"
updates_subscription_name = "updates-subscription-sub"
health_check_subscription_name = "health-check-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
crawl_topic_path = publisher.topic_path(project_id, crawl_topic_name)
search_topic_path = publisher.topic_path(project_id, search_topic_name)
updates_subscription_path = subscriber.subscription_path(project_id, updates_subscription_name)
health_check_subscription_path = subscriber.subscription_path(project_id, health_check_subscription_name)

app = Flask(__name__)

connected_clients = {}
subscriptions = {}  # crawl_query_id to list of websockets
active_machines = {}  # machine_type: machine_id: last_heartbeat_time
message_queue = Queue()  # Thread-safe queue for WebSocket messages

async def websocket_handler(websocket, path):
    logging.info("New WebSocket connection established")
    connected_clients[websocket] = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logging.info(f"Received WebSocket message: {data}")
                if data["type"] == "subscribe":
                    crawl_query_id = data["crawl_query_id"]
                    connected_clients[websocket] = crawl_query_id
                    subscriptions.setdefault(crawl_query_id, []).append(websocket)
                    logging.info(f"Client subscribed to {crawl_query_id}")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse WebSocket message: {e}")
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
    finally:
        if connected_clients[websocket]:
            subscriptions[connected_clients[websocket]].remove(websocket)
        del connected_clients[websocket]
        logging.info("WebSocket connection closed")

async def process_message_queue():
    while True:
        try:
            # Get message from the queue (non-blocking)
            message_data = message_queue.get_nowait()
            crawl_query_id = message_data.get("crawl_query_id")  # Extract crawl_query_id if present
            if crawl_query_id and crawl_query_id in subscriptions:
                for client in subscriptions[crawl_query_id]:
                    await client.send(json.dumps(message_data))
                    logging.info(f"Sent to client for {crawl_query_id}: {message_data}")
            elif "type" in message_data and message_data["type"] in ["machine_active", "machine_dead"]:
                # Broadcast machine status updates to all clients
                for client in connected_clients:
                    await client.send(json.dumps(message_data))
                    logging.info(f"Sent to all clients: {message_data}")
            message_queue.task_done()
        except Empty:  # Use the imported Empty exception
            await asyncio.sleep(0.1)  # Sleep briefly if queue is empty
        except Exception as e:
            logging.error(f"Error processing message queue: {e}")

start_server = websockets.serve(websocket_handler, "0.0.0.0", 8765)

def updates_callback(message):
    logging.info(f"Received Pub/Sub message: {message.data.decode('utf-8')}")
    try:
        data = json.loads(message.data.decode('utf-8'))
        message.ack()
        crawl_query_id = data.get("crawl_query_id")
        if crawl_query_id and crawl_query_id in subscriptions:
            message_queue.put(data)
            logging.info(f"Queued update for {crawl_query_id}: {data}")
        else:
            logging.info(f"No subscribers for crawl_query_id: {crawl_query_id}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse Pub/Sub message: {e}")
        message.nack()

def health_check_callback(message):
    logging.debug(f"Received raw message: {message.data.decode('utf-8')}")
    try:
        data = json.loads(message.data.decode('utf-8'))
        logging.debug(f"Parsed message data: {data}")
        message.ack()
        if data["type"] == "heartbeat":
            machine_type = data["machine_type"]
            machine_id = data["machine_id"]
            active_machines.setdefault(machine_type, {})[machine_id] = time.time()
            message_queue.put({
                "type": "machine_active",
                "machine_type": machine_type,
                "machine_id": machine_id
            })
            logging.info(f"Sent machine_active for {machine_type} {machine_id}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse heartbeat message: {e}")
        message.nack()

def monitor_machines():
    while True:
        current_time = time.time()
        for machine_type, machines in active_machines.items():
            for machine_id, last_time in list(machines.items()):
                if current_time - last_time > 20:
                    del machines[machine_id]
                    message_queue.put({
                        "type": "machine_dead",
                        "machine_type": machine_type,
                        "machine_id": machine_id
                    })
                    logging.info(f"Sent machine_dead for {machine_type} {machine_id}")
        time.sleep(5)

def start_health_check_subscription():
    streaming_pull_future = subscriber.subscribe(health_check_subscription_path, callback=health_check_callback)
    logging.info(f"Started health check subscription on {health_check_subscription_path}")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logging.info("Health check subscription cancelled")

def start_updates_subscription():
    streaming_pull_future = subscriber.subscribe(updates_subscription_path, callback=updates_callback)
    logging.info(f"Started updates subscription on {updates_subscription_path}")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logging.info("Updates subscription cancelled")

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

@app.route('/search', methods=['POST'])
def search():
    data = request.json
    search_task = {"query_text": data['keyword'], "crawl_query_id": data['uuid']}
    publisher.publish(search_topic_path, json.dumps(search_task).encode('utf-8'))
    logging.info(f"Published search task: {json.dumps(search_task)}")
    return jsonify({"status": "search initiated"})

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_server)
    threading.Thread(target=loop.run_forever, daemon=True).start()
    threading.Thread(target=monitor_machines, daemon=True).start()
    threading.Thread(target=start_health_check_subscription, daemon=True).start()
    threading.Thread(target=start_updates_subscription, daemon=True).start()
    loop.create_task(process_message_queue())
    app.run(host='0.0.0.0', port=5000)
