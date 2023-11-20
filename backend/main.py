from typing import List    
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import json
import time
from services.kafka_service import create_a_producer, create_a_consumer
from services.data_stream import data_stream
from confluent_kafka import  KafkaError
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC

import asyncio

app = FastAPI()

# Middleware for handling Cross Origin Resource Sharing (CORS)
app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # can alter with time
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Class for managing WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    # Method to accept and add a new WebSocket connection to the active connections list
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    # Method to remove a WebSocket connection from the active connections list
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    # Method to send a message to a specific WebSocket connection
    async def stream_ticks(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    # Method to broadcast a message to all active WebSocket connections
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# Home endpoint
@app.get("/")
def home():
    return "Welcome Home"

# WebSocket endpoint
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    # Connect the WebSocket
    await manager.connect(websocket)
    # Create a Kafka producer
    producer = create_a_producer()
    # Create a Kafka consumer
    consumer = create_a_consumer()

    # Get the current time
    now = datetime.now()
    current_time = now.strftime("%H:%M")


    try:
        # Start the data stream task
        asyncio.create_task(data_stream(producer))
        print('after sending the message')

        topics = [KAFKA_TOPIC]
        consumer.subscribe(topics)

        while True:
            msg = consumer
            if msg:
                print('Received message: {}'.format(msg))
            else : 
                print('lavda ka msg')
        # Loop over messages from the Kafka consumer

            # for msg in consumer:
            #     print(f'Received msg: {msg.value}')
        #     # Decode the received message
        #     decoded_msg = msg.value.decode('utf-8')
        #     # Send the decoded message to the WebSocket connection
        #     await manager.stream_ticks(decoded_msg, websocket)
    except WebSocketDisconnect:
        # Handle WebSocket disconnection
        manager.disconnect(websocket)
        # Create a message indicating the client is offline
        message = {"time": current_time, "clientId": client_id, "message": "Offline"}
        # Broadcast the offline message
        await manager.broadcast(json.dumps(message))
        # Stop the Kafka producer
        # producer.flush()
        # Stop the Kafka consumer
        consumer.close()
    except Exception as e:
        # Handle any other exceptions
        print(f"An error occurred: {e}")
        # Stop the Kafka producer
        # producer.flush()
        # Stop the Kafka consumer
        consumer.close()
