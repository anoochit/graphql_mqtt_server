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


@strawberry.type
class TopicInfo:
    name: str
    message_count: int
    last_message_time: Optional[datetime] = None
    is_subscribed: bool = False


@strawberry.input
class SendToTopicInput:
    topic: str
    content: str
    sender: Optional[str] = None


# In-memory storage (replace with database in production)
messages_store: List[Message] = []
mqtt_messages_queue = asyncio.Queue(maxsize=1000)  # Increase queue size


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
    
    def connect_to_broker(self, host="mosquitto", port=1883):
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
    
    def unsubscribe_from_topic(self, topic: str):
        with self._topics_lock:
            self.subscribed_topics.discard(topic)
        
        if self.client.is_connected():
            self.client.unsubscribe(topic)
            print(f"Unsubscribed from topic: {topic}")
    
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
    
    def get_subscribed_topics(self):
        with self._topics_lock:
            return list(self.subscribed_topics)


# Initialize MQTT handler
mqtt_handler = MQTTHandler()


# GraphQL Schema - Refactored with topic-focused operations
@strawberry.type
class Query:
    @strawberry.field
    def query_topic(self, topic: str, limit: int = 50) -> List[Message]:
        """Query messages from a specific topic"""
        print(f"Querying topic: {topic}, Total messages in store: {len(messages_store)}")
        topic_messages = [msg for msg in messages_store if msg.topic == topic]
        print(f"Found {len(topic_messages)} messages for topic: {topic}")
        return sorted(topic_messages, key=lambda x: x.timestamp, reverse=True)[:limit]
    
    @strawberry.field
    def query_all_topics(self) -> List[TopicInfo]:
        """Get information about all available topics"""
        print(f"Total messages in store: {len(messages_store)}")
        topic_stats = {}
        
        # Gather statistics for each topic
        for msg in messages_store:
            if msg.topic not in topic_stats:
                topic_stats[msg.topic] = {
                    'count': 1,
                    'last_time': msg.timestamp
                }
            else:
                topic_stats[msg.topic]['count'] += 1
                if msg.timestamp > topic_stats[msg.topic]['last_time']:
                    topic_stats[msg.topic]['last_time'] = msg.timestamp
        
        # Get subscribed topics
        subscribed_topics = set(mqtt_handler.get_subscribed_topics())
        
        # Create TopicInfo objects
        topic_infos = []
        for topic_name, stats in topic_stats.items():
            topic_infos.append(TopicInfo(
                name=topic_name,
                message_count=stats['count'],
                last_message_time=stats['last_time'],
                is_subscribed=topic_name in subscribed_topics
            ))
        
        print(f"Found {len(topic_infos)} topics: {[t.name for t in topic_infos]}")
        return sorted(topic_infos, key=lambda x: x.last_message_time or datetime.min, reverse=True)
    
    @strawberry.field
    def query_subscribed_topics(self) -> List[str]:
        """Get all currently subscribed MQTT topics"""
        return mqtt_handler.get_subscribed_topics()


@strawberry.type
class Mutation:
    @strawberry.mutation
    def send_to_topic(self, input: SendToTopicInput) -> Message:
        """Send a message to a specific MQTT topic"""
        print(f"Sending message to topic: {input.topic}")
        
        # Create message payload with metadata
        message_payload = {
            "content": input.content,
            "sender": input.sender or "graphql_client",
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to MQTT (the message will be handled by on_message callback)
        success = mqtt_handler.publish_message(
            input.topic, 
            json.dumps(message_payload)
        )
        
        if success:
            print(f"Message successfully sent to topic: {input.topic}")
            # Create the message object
            message = Message(
                id=message_payload["id"],
                topic=input.topic,
                content=input.content,
                timestamp=datetime.fromisoformat(message_payload["timestamp"]),
                sender=message_payload["sender"]
            )
            
            # Also store locally in case MQTT callback doesn't work
            messages_store.append(message)
            
            # Add to queue for subscriptions
            try:
                mqtt_messages_queue.put_nowait(message)
            except asyncio.QueueFull:
                print("Queue full when adding sent message")
            
            return message
        else:
            raise Exception(f"Failed to send message to topic: {input.topic}")
    
    @strawberry.mutation
    def subscribe_to_topic(self, topic: str) -> str:
        """Subscribe to an MQTT topic for real-time messages"""
        mqtt_handler.subscribe_to_topic(topic)
        return f"Successfully subscribed to topic: {topic}"
    
    @strawberry.mutation
    def unsubscribe_from_topic(self, topic: str) -> str:
        """Unsubscribe from an MQTT topic"""
        mqtt_handler.unsubscribe_from_topic(topic)
        return f"Successfully unsubscribed from topic: {topic}"
    
    @strawberry.mutation
    def clear_topic_messages(self, topic: str) -> str:
        """Clear all messages from a specific topic"""
        global messages_store
        initial_count = len(messages_store)
        messages_store = [msg for msg in messages_store if msg.topic != topic]
        cleared_count = initial_count - len(messages_store)
        return f"Cleared {cleared_count} messages from topic: {topic}"
    
    @strawberry.mutation
    def clear_all_messages(self) -> str:
        """Clear all messages from all topics"""
        global messages_store
        cleared_count = len(messages_store)
        messages_store.clear()
        return f"Cleared {cleared_count} messages from all topics"
    
    @strawberry.mutation
    def create_test_data(self) -> str:
        """Create some test messages for testing purposes"""
        global messages_store
        
        test_messages = [
            Message(
                id=str(uuid.uuid4()),
                topic="test/messages",
                content="Hello from test topic",
                timestamp=datetime.now(),
                sender="test_user"
            ),
            Message(
                id=str(uuid.uuid4()),
                topic="sensors/temperature",
                content="25.5°C",
                timestamp=datetime.now(),
                sender="sensor_01"
            ),
            Message(
                id=str(uuid.uuid4()),
                topic="alerts/system",
                content="System online",
                timestamp=datetime.now(),
                sender="system"
            )
        ]
        
        messages_store.extend(test_messages)
        
        # Also add to queue for subscriptions
        for message in test_messages:
            try:
                mqtt_messages_queue.put_nowait(message)
            except asyncio.QueueFull:
                print("Queue full when adding test message")
        
        return f"Created {len(test_messages)} test messages"


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def subscribe_topic_messages(self, topic: str) -> AsyncGenerator[Message, None]:
        """Subscribe to real-time messages from a specific MQTT topic"""
        print(f"Starting subscription for topic: {topic}")
        
        try:
            while True:
                try:
                    # Wait for messages from the global queue with a timeout
                    message = await asyncio.wait_for(mqtt_messages_queue.get(), timeout=1.0)
                    
                    # Only yield messages from the specified topic
                    if message.topic == topic:
                        print(f"Yielding message: {message.content} from topic: {message.topic}")
                        yield message
                    else:
                        # Put the message back if it's not for this topic
                        try:
                            mqtt_messages_queue.put_nowait(message)
                        except asyncio.QueueFull:
                            print("Queue full, dropping message for other topic")
                            
                except asyncio.TimeoutError:
                    # No message received within timeout, continue listening
                    continue
                    
                except asyncio.QueueEmpty:
                    # No messages in queue, wait a bit
                    await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            print(f"Subscription cancelled for topic: {topic}")
            raise
        except Exception as e:
            print(f"Error in topic subscription for {topic}: {e}")
            await asyncio.sleep(1)
    
    @strawberry.subscription
    async def subscribe_all_topic_messages(self) -> AsyncGenerator[Message, None]:
        """Subscribe to real-time messages from all MQTT topics"""
        print("Starting subscription for all topics")
        
        try:
            while True:
                try:
                    # Wait for messages from the global queue with a timeout
                    message = await asyncio.wait_for(mqtt_messages_queue.get(), timeout=1.0)
                    print(f"Yielding message: {message.content} from topic: {message.topic}")
                    yield message
                        
                except asyncio.TimeoutError:
                    # No message received within timeout, continue listening
                    continue
                    
                except asyncio.QueueEmpty:
                    # No messages in queue, wait a bit
                    await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            print("Subscription cancelled for all topics")
            raise
        except Exception as e:
            print(f"Error in all topics subscription: {e}")
            await asyncio.sleep(1)
    
    @strawberry.subscription
    async def subscribe_topic_activity(self) -> AsyncGenerator[str, None]:
        """Subscribe to notifications about topic activity changes"""
        last_topics = set()
        print("Starting topic activity subscription")
        
        try:
            while True:
                await asyncio.sleep(2)  # Check every 2 seconds
                current_topics = set(msg.topic for msg in messages_store)
                
                # Check for new topics
                new_topics = current_topics - last_topics
                if new_topics:
                    for topic in new_topics:
                        activity_message = f"New topic detected: {topic}"
                        print(f"Yielding activity: {activity_message}")
                        yield activity_message
                
                last_topics = current_topics
                    
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
    subscribed_topics = mqtt_handler.get_subscribed_topics()
    
    return {
        "message": "GraphQL MQTT Server",
        "graphql_endpoint": "/graphql",
        "mqtt_subscribed_topics": subscribed_topics,
        "total_message_count": len(messages_store),
        "available_topics": list(set(msg.topic for msg in messages_store))
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