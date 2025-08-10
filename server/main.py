import asyncio
import json
from typing import List, Optional, AsyncGenerator
from datetime import datetime
import uuid
from contextlib import asynccontextmanager
import threading

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL
from fastapi import FastAPI
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import uvicorn


# Data models
@strawberry.type
class Message:
    id: str
    topic: str
    content: str
    timestamp: datetime
    sender: Optional[str] = None


@strawberry.input
class MessageInput:
    topic: str
    content: str
    sender: Optional[str] = None


# In-memory storage (replace with database in production)
messages_store: List[Message] = []
mqtt_messages_queue = asyncio.Queue()


# MQTT Client Setup
class MQTTHandler:
    def __init__(self):
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"graphql_server_{uuid.uuid4()}"
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.subscribed_topics = set()
        self._topics_lock = threading.Lock()  # Thread-safe access to topics
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected to MQTT broker with result code {reason_code}")
        # Subscribe to all topics that were requested
        # Use thread-safe access to subscribed topics
        with self._topics_lock:
            topics_to_subscribe = list(self.subscribed_topics)
        
        for topic in topics_to_subscribe:
            client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
    
    def on_message(self, client, userdata, msg):
        try:
            # Decode the message
            content = msg.payload.decode('utf-8')
            
            # Try to parse as JSON (for messages sent via GraphQL)
            try:
                message_data = json.loads(content)
                if isinstance(message_data, dict) and all(k in message_data for k in ['content', 'sender', 'id', 'timestamp']):
                    # This is a structured message from GraphQL
                    message = Message(
                        id=message_data["id"],
                        topic=msg.topic,
                        content=message_data["content"],
                        timestamp=datetime.fromisoformat(message_data["timestamp"]),
                        sender=message_data["sender"]
                    )
                else:
                    # JSON but not our format, treat as plain content
                    message = Message(
                        id=str(uuid.uuid4()),
                        topic=msg.topic,
                        content=content,
                        timestamp=datetime.now(),
                        sender="external_client"
                    )
            except json.JSONDecodeError:
                # Plain text message from external MQTT client
                message = Message(
                    id=str(uuid.uuid4()),
                    topic=msg.topic,
                    content=content,
                    timestamp=datetime.now(),
                    sender="external_client"
                )
            
            # Store the message
            messages_store.append(message)
            
            # Add to queue for subscriptions (non-blocking)
            try:
                mqtt_messages_queue.put_nowait(message)
                print(f"Added message to queue: {message.content}")
            except asyncio.QueueFull:
                print("Message queue is full, skipping...")
            
            print(f"Received message from {msg.topic}: {message.content} (sender: {message.sender})")
            
        except Exception as e:
            print(f"Error processing MQTT message: {e}")
    
    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        print(f"Disconnected from MQTT broker with result code {reason_code}")
    
    def connect_to_broker(self, host="localhost", port=1883):
        try:
            self.client.connect(host, port, 60)
            self.client.loop_start()
            print(f"Attempting to connect to MQTT broker at {host}:{port}")
        except Exception as e:
            print(f"Failed to connect to MQTT broker: {e}")
    
    def subscribe_to_topic(self, topic: str):
        with self._topics_lock:
            self.subscribed_topics.add(topic)
        
        if self.client.is_connected():
            self.client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
    
    def publish_message(self, topic: str, message: str):
        try:
            result = self.client.publish(topic, message)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"Message published to {topic}: {message}")
                return True
            else:
                print(f"Failed to publish message to {topic}")
                return False
        except Exception as e:
            print(f"Error publishing message: {e}")
            return False


# Initialize MQTT handler
mqtt_handler = MQTTHandler()


# GraphQL Schema
@strawberry.type
class Query:
    @strawberry.field
    def messages(self, topic: Optional[str] = None, limit: int = 50) -> List[Message]:
        """Get messages, optionally filtered by topic"""
        filtered_messages = messages_store
        
        if topic:
            filtered_messages = [msg for msg in messages_store if msg.topic == topic]
        
        # Return the most recent messages
        return sorted(filtered_messages, key=lambda x: x.timestamp, reverse=True)[:limit]
    
    @strawberry.field
    def topics(self) -> List[str]:
        """Get all available topics"""
        return list(set(msg.topic for msg in messages_store))
    
    @strawberry.field
    def message_count(self, topic: Optional[str] = None) -> int:
        """Get message count, optionally for a specific topic"""
        if topic:
            return len([msg for msg in messages_store if msg.topic == topic])
        return len(messages_store)


@strawberry.type
class Mutation:
    @strawberry.mutation
    def send_message(self, message_input: MessageInput) -> Message:
        """Send a message to an MQTT topic"""
        # Create message payload with metadata
        message_payload = {
            "content": message_input.content,
            "sender": message_input.sender or "graphql_client",
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to MQTT (the message will be handled by on_message callback)
        success = mqtt_handler.publish_message(
            message_input.topic, 
            json.dumps(message_payload)
        )
        
        if success:
            # Return the message object (actual storage happens in on_message)
            return Message(
                id=message_payload["id"],
                topic=message_input.topic,
                content=message_input.content,
                timestamp=datetime.fromisoformat(message_payload["timestamp"]),
                sender=message_payload["sender"]
            )
        else:
            raise Exception("Failed to publish message to MQTT topic")
    
    @strawberry.mutation
    def subscribe_to_mqtt_topic(self, topic: str) -> str:
        """Subscribe to an MQTT topic"""
        mqtt_handler.subscribe_to_topic(topic)
        return f"Subscribed to topic: {topic}"
    
    @strawberry.mutation
    def clear_messages(self, topic: Optional[str] = None) -> str:
        """Clear messages, optionally for a specific topic"""
        global messages_store
        
        if topic:
            messages_store = [msg for msg in messages_store if msg.topic != topic]
            return f"Cleared messages for topic: {topic}"
        else:
            messages_store.clear()
            return "Cleared all messages"


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def message_stream(self, topic: Optional[str] = None) -> AsyncGenerator[Message, None]:
        """Subscribe to real-time messages from MQTT topics"""
        print(f"Starting subscription for topic: {topic}")
        
        # Create a separate queue for this subscription
        local_queue = asyncio.Queue()
        
        # Keep track of initial message count to detect new messages
        initial_count = len(messages_store)
        
        try:
            while True:
                # Check for new messages in the global store
                current_count = len(messages_store)
                if current_count > initial_count:
                    # Get new messages since last check
                    new_messages = messages_store[initial_count:]
                    
                    for message in new_messages:
                        # Filter by topic if specified
                        if topic is None or message.topic == topic:
                            print(f"Yielding message: {message.content} from topic: {message.topic}")
                            yield message
                    
                    initial_count = current_count
                
                # Also try to get messages from the global queue
                try:
                    # Non-blocking check for new messages from MQTT
                    message = mqtt_messages_queue.get_nowait()
                    
                    # Filter by topic if specified
                    if topic is None or message.topic == topic:
                        print(f"Yielding queued message: {message.content} from topic: {message.topic}")
                        yield message
                        
                except asyncio.QueueEmpty:
                    # No messages in queue, continue
                    pass
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            print("Subscription cancelled")
            raise
        except Exception as e:
            print(f"Error in message stream subscription: {e}")
            # Continue the subscription even if there's an error
            await asyncio.sleep(1)
    
    @strawberry.subscription
    async def topic_activity(self) -> AsyncGenerator[str, None]:
        """Subscribe to topic activity notifications"""
        last_count = len(set(msg.topic for msg in messages_store))
        print("Starting topic activity subscription")
        
        try:
            while True:
                await asyncio.sleep(2)  # Check every 2 seconds
                current_topics = set(msg.topic for msg in messages_store)
                current_count = len(current_topics)
                
                if current_count != last_count:
                    activity_message = f"Topic count changed: {current_count} active topics"
                    print(f"Yielding activity: {activity_message}")
                    yield activity_message
                    last_count = current_count
                    
        except asyncio.CancelledError:
            print("Topic activity subscription cancelled")
            raise
        except Exception as e:
            print(f"Error in topic activity subscription: {e}")
            await asyncio.sleep(1)


# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting up GraphQL MQTT Server...")
    
    # Connect to MQTT broker (change host/port as needed)
    mqtt_handler.connect_to_broker(host="localhost", port=1883)
    
    # Subscribe to some default topics for testing
    mqtt_handler.subscribe_to_topic("test/messages")
    mqtt_handler.subscribe_to_topic("sensors/+")  # Wildcard subscription
    
    print("GraphQL MQTT Server started!")
    print("GraphQL endpoint: http://localhost:8000/graphql")
    print("GraphQL subscriptions: ws://localhost:8000/graphql")
    
    yield
    
    # Shutdown
    print("Shutting down...")
    mqtt_handler.client.loop_stop()
    mqtt_handler.client.disconnect()
    print("MQTT connection closed")


# Create FastAPI app with lifespan handler
app = FastAPI(
    title="GraphQL MQTT Server",
    description="A GraphQL server with MQTT integration for real-time messaging",
    lifespan=lifespan
)

# Create GraphQL schema
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription
)

# Add GraphQL router
graphql_app = GraphQLRouter(
    schema,
    subscription_protocols=[
        GRAPHQL_TRANSPORT_WS_PROTOCOL,
        GRAPHQL_WS_PROTOCOL,
    ],
)

app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
async def root():
    """Root endpoint with basic info"""
    with mqtt_handler._topics_lock:
        subscribed_topics = list(mqtt_handler.subscribed_topics)
    
    return {
        "message": "GraphQL MQTT Server",
        "graphql_endpoint": "/graphql",
        "mqtt_topics": subscribed_topics,
        "message_count": len(messages_store)
    }


if __name__ == "__main__":
    print("Starting GraphQL MQTT Server...")
    print("Make sure you have an MQTT broker running (e.g., mosquitto)")
    print("Install dependencies: pip install strawberry-graphql[fastapi] paho-mqtt uvicorn")
    
    uvicorn.run(
        "main:app",  # Change to your filename if different
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
