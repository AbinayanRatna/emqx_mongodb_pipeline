import json
import ssl
from datetime import datetime
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
from pymongo import MongoClient


# ==========================================
# MQTT CONFIGURATION
# ==========================================
MQTT_BROKER = ""
MQTT_PORT = 8883  # 1883 for non-TLS
MQTT_USERNAME = ""
MQTT_PASSWORD = ""

TOPICS = [
    ("topic/#", 1),
]

# ==========================================
# MONGODB CONFIGURATION
# ==========================================
MONGO_URI = ""
DATABASE_NAME = ""
COLLECTION_NAME = ""

# ==========================================
# MongoDB Setup
# ==========================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]


def save_to_mongodb(topic, payload):
    """
    Save MQTT message into MongoDB
    """
    topic_suffix = topic.split("/", 1)[1] if "/" in topic else topic
    document = {
        "topic": topic_suffix,
        "received_at": datetime.now(ZoneInfo("Asia/Colombo")).isoformat(),
        "payload": payload
    }

    result = collection.insert_one(document)
    print(f"Saved document: {result.inserted_id}")


# ==========================================
# MQTT Callbacks
# ==========================================
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("Connected to MQTT Broker")

        for topic, qos in TOPICS:
            client.subscribe(topic, qos)
            print(f"Subscribed: {topic}")
    else:
        print(f"Connection failed: {reason_code}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print(f"Disconnected from MQTT Broker: {reason_code}")


def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")

        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"raw_message": payload_str}

        print(f"\nTopic: {msg.topic}")
        print(f"Payload: {payload}")

        save_to_mongodb(msg.topic, payload)

    except Exception as e:
        print(f"Error processing message: {e}")


# ==========================================
# MQTT Client Setup
# ==========================================
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mongo-subscriber"
)

client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Enable TLS if using 8883
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.tls_insecure_set(False)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Auto reconnect settings
client.reconnect_delay_set(min_delay=1, max_delay=60)

# ==========================================
# Start MQTT Consumer
# ==========================================
print("Connecting to MQTT Broker...")

client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

client.loop_forever()