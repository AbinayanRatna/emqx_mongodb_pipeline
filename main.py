import asyncio
import json
import os
import ssl
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MQTT_BROKER = "i3952631.ala.asia-southeast1.emqxsl.com"
MQTT_PORT = 8883
MQTT_USERNAME = "fyp_user"
MQTT_PASSWORD = "Cjed5Tdva52JbA4"
TOPIC = "gateway/#"

MONGO_URI = "mongodb+srv://viyanu:Aa123456789@cluster0.6j9cx5s.mongodb.net"
DATABASE_NAME = "fyp"
COLLECTION_NAME = "mqtt"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

active_connections = set()
loop = None

def get_cap_voltage_pct(payload):
    data = payload.get("data", {})
    cap_voltage = None
    if isinstance(data, dict):
        cap_voltage = data.get("cap_voltage")
    if cap_voltage is None:
        cap_voltage = payload.get("cap_voltage")
    if cap_voltage is not None:
        try:
            val = float(cap_voltage)
            return round(max(0.0, min(100.0, (val / 5.4) * 100.0)), 2)
        except (ValueError, TypeError):
            pass
    return None

def serialize_doc(doc):
    payload = doc.get("payload", {})
    cap_voltage_pct = get_cap_voltage_pct(payload)
    return {
        "id": str(doc["_id"]),
        "topic": doc.get("topic"),
        "received_at": doc.get("received_at"),
        "payload": payload,
        "cap_voltage_pct": cap_voltage_pct
    }

def save_to_mongodb(topic, payload):
    topic_suffix = topic.split("/", 1)[1] if "/" in topic else topic
    document = {
        "topic": topic_suffix,
        "received_at": datetime.now(ZoneInfo("Asia/Colombo")).isoformat(),
        "payload": payload
    }
    result = collection.insert_one(document)
    return document

async def broadcast_mqtt_message(topic, payload):
    if not active_connections:
        return
    topic_suffix = topic.split("/", 1)[1] if "/" in topic else topic
    cap_voltage_pct = get_cap_voltage_pct(payload)
    message = {
        "topic": topic_suffix,
        "received_at": datetime.now(ZoneInfo("Asia/Colombo")).isoformat(),
        "payload": payload,
        "cap_voltage_pct": cap_voltage_pct
    }
    data_str = json.dumps(message)
    for websocket in list(active_connections):
        try:
            await websocket.send_text(data_str)
        except Exception:
            active_connections.discard(websocket)

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        client.subscribe(TOPIC, 1)

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    pass

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"raw_message": payload_str}
        save_to_mongodb(msg.topic, payload)
        if loop is not None:
            asyncio.run_coroutine_threadsafe(broadcast_mqtt_message(msg.topic, payload), loop)
    except Exception:
        pass

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mongo-subscriber"
)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.tls_insecure_set(False)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.reconnect_delay_set(min_delay=1, max_delay=60)

@app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_running_loop()
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

@app.on_event("shutdown")
async def shutdown_event():
    client.loop_stop()
    client.disconnect()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.discard(websocket)
    except Exception:
        active_connections.discard(websocket)

@app.get("/api/devices")
async def get_devices():
    try:
        devices = collection.distinct("topic")
        return {"devices": devices}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/devices/{device_id}/history")
async def get_device_history(device_id: str):
    try:
        cursor = collection.find({"topic": device_id}).sort("received_at", -1).limit(50)
        history = [serialize_doc(doc) for doc in cursor]
        history.reverse()
        return {"history": history}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/devices/{device_id}/range")
async def get_device_history_range(device_id: str, start: str, end: str):
    try:
        query = {
            "topic": device_id,
            "received_at": {
                "$gte": start,
                "$lte": end
            }
        }
        cursor = collection.find(query).sort("received_at", 1)
        history = [serialize_doc(doc) for doc in cursor]
        return {"history": history}
    except Exception as e:
        return {"error": str(e)}

class LoginRequest(BaseModel):
    userid: str
    password: str

@app.post("/api/login")
async def login(request: LoginRequest):
    if request.userid == "admin@zenode" and request.password == "zenode@fyp":
        return {"success": True, "token": "zenode-session-token-abc123xyz"}
    return {"success": False, "error": "Invalid User ID or Password"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)