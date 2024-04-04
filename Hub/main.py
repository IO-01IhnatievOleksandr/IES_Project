import logging
from typing import List
from fastapi import FastAPI
from redis import Redis
import json
import paho.mqtt.client as mqtt
from app.adapters.store_api_adapter import StoreApiAdapter
from app.entities.processed_agent_data import ProcessedAgentData
from config import STORE_API_BASE_URL, REDIS_HOST, REDIS_PORT, BATCH_SIZE, MQTT_TOPIC, MQTT_BROKER_HOST, \
MQTT_BROKER_PORT


# Configure logging settings
logging.basicConfig(
    level=logging.INFO, # Set the log level to INFO (you can use logging.DEBUG for more detailed logs)
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s",
    handlers=[
        logging.StreamHandler(), # Output log messages to the console
        logging.FileHandler("app.log"), # Save log messages to a file
    ],
)

logging.info("\n---STARTING HUB---\n")

# Create an instance of the Redis using the configuration
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)

# Create an instance of the StoreApiAdapter using the configuration
store_adapter = StoreApiAdapter(api_base_url=STORE_API_BASE_URL)

# Create an instance of the AgentMQTTAdapter using the configuration


# FastAPI
app = FastAPI()

# MQTT
client = mqtt.Client()

global test


@app.post("/processed_agent_data/")
async def save_processed_agent_data(processed_agent_data:ProcessedAgentData):
    logging.info("---POST---")
    print(processed_agent_data)
    redis_client.lpush("processed_agent_data", processed_agent_data.model_dump_json())
    processed_agent_data_batch: List[ProcessedAgentData] = []
    global test
    if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
        for _ in range(BATCH_SIZE):
            processed_agent_data = ProcessedAgentData.model_validate_json(
                redis_client.lpop("processed_agent_data"))
            processed_agent_data_batch.append(processed_agent_data)
        store_adapter.save_data(
        processed_agent_data_batch=processed_agent_data_batch)
        publish_messages(client, MQTT_TOPIC, processed_agent_data_batch)
        # Clear the Redis queue after processing the batch
        test = True
        redis_client.delete("processed_agent_data")      
        print("data after deleting\n", redis_client.lpop("processed_agent_data"))
    return {"status": "ok"}


def publish_messages(client, topic, messages):
    for message in [json.loads(item.json()) for item in messages]:
        text = json.dumps(message)
        print("Data to mqtt", text)
        client.publish(topic, text)


def on_connect(client, userdata, flags, rc):
    logging.info("---CONNECT---")
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.info(f"Failed to connect to MQTT broker with code: {rc}")


def on_message(client, userdata, msg):
    global test
    try:
        payload: str = msg.payload.decode("utf-8")
        processed_agent_data = ProcessedAgentData.model_validate_json(payload, strict=True)
        redis_client.lpush("processed_agent_data", processed_agent_data.model_dump_json())
        if redis_client.llen("processed_agent_data") >= BATCH_SIZE and not test:
            processed_agent_data_batch = []
            for _ in range(BATCH_SIZE):
                processed_agent_data = ProcessedAgentData.model_validate_json(
                    redis_client.lpop("processed_agent_data"))
                processed_agent_data_batch.append(processed_agent_data)
            publish_messages(client, MQTT_TOPIC, processed_agent_data_batch)
            store_adapter.save_data(processed_agent_data_batch=processed_agent_data_batch)         
            test = True
    except Exception as e:
        logging.info(f"Error processing MQTT message: {e}")
    finally:
        test = False

# Connect
logging.info("CONNECTION")
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
# Start
client.loop_start()