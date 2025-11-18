# Description: Real-time streaming system for live metric updates
# Description: Supports WebSocket, SSE, pub/sub with rate limiting and reconnection handling

"""
Real-Time Streaming System

Provides multiple protocols for streaming metric updates:
1. WebSocket - Bidirectional communication for live metrics
2. Server-Sent Events (SSE) - One-way streaming for updates
3. Pub/Sub - Broadcast pattern for metric changes
4. Rate Limiting - Prevent client/server overload
5. Reconnection - State management for client reconnections

All streams support filtering, aggregation, and backpressure handling.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, AsyncIterator, Callable
from dataclasses import dataclass, field, asdict
from collections import deque
from enum import Enum
import logging

from fastapi import WebSocket, WebSocketDisconnect
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class StreamProtocol(Enum):
    """Streaming protocol types."""
    WEBSOCKET = "websocket"
    SSE = "sse"
    PUBSUB = "pubsub"


class MessageType(Enum):
    """Message types for streaming."""
    METRIC_UPDATE = "metric_update"
    ALERT = "alert"
    HEARTBEAT = "heartbeat"
    SUBSCRIPTION_CONFIRMED = "subscription_confirmed"
    ERROR = "error"
    RECONNECT = "reconnect"


@dataclass
class StreamMessage:
    """Standard message format for all streams."""
    message_type: MessageType
    timestamp: datetime
    data: Dict[str, Any]
    message_id: Optional[str] = None
    sequence: Optional[int] = None

    def __post_init__(self):
        if self.message_id is None:
            self.message_id = str(uuid.uuid4())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "type": self.message_type.value,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "id": self.message_id,
            "sequence": self.sequence
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)


@dataclass
class ClientSubscription:
    """Client subscription configuration."""
    client_id: str
    metric_names: Optional[List[str]] = None
    resource_filters: Optional[Dict[str, Any]] = None
    min_interval_ms: int = 100  # Minimum time between messages
    max_buffer_size: int = 1000  # Maximum buffered messages
    protocol: StreamProtocol = StreamProtocol.WEBSOCKET


@dataclass
class ClientState:
    """Client connection state for reconnection."""
    client_id: str
    last_sequence: int = 0
    last_message_id: Optional[str] = None
    connected_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    subscription: Optional[ClientSubscription] = None
    message_buffer: deque = field(default_factory=lambda: deque(maxlen=1000))


class RateLimiter:
    """
    Token bucket rate limiter for stream messages.

    Prevents overwhelming clients with too many messages.
    """

    def __init__(
        self,
        rate: int = 100,  # Messages per second
        burst: int = 200   # Maximum burst size
    ):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens.

        Returns True if tokens available, False otherwise.
        """
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            # Add tokens based on elapsed time
            self.tokens = min(
                self.burst,
                self.tokens + elapsed * self.rate
            )

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            return False

    async def wait(self, tokens: int = 1):
        """Wait until tokens available (with backpressure)."""
        while not await self.acquire(tokens):
            await asyncio.sleep(0.01)  # 10ms backoff


class MessageBroker:
    """
    Pub/Sub message broker for broadcasting metric updates.

    Uses Redis for distributed pub/sub across multiple instances.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        use_redis: bool = True
    ):
        self.redis_url = redis_url
        self.use_redis = use_redis
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None

        # In-memory pub/sub for single instance or fallback
        self.local_subscribers: Dict[str, Set[Callable]] = {}
        self.local_lock = asyncio.Lock()

    async def connect(self):
        """Connect to Redis."""
        if self.use_redis:
            try:
                self.redis_client = await redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True
                )
                self.pubsub = self.redis_client.pubsub()
                logger.info("Connected to Redis for pub/sub")
            except Exception as e:
                logger.warning(f"Redis connection failed, using local pub/sub: {e}")
                self.use_redis = False

    async def disconnect(self):
        """Disconnect from Redis."""
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.close()

    async def publish(self, channel: str, message: StreamMessage):
        """Publish message to channel."""
        payload = message.to_json()

        if self.use_redis and self.redis_client:
            await self.redis_client.publish(channel, payload)
        else:
            # Local pub/sub
            async with self.local_lock:
                if channel in self.local_subscribers:
                    for callback in self.local_subscribers[channel]:
                        try:
                            await callback(message)
                        except Exception as e:
                            logger.error(f"Error in subscriber callback: {e}")

    async def subscribe(
        self,
        channel: str,
        callback: Callable[[StreamMessage], None]
    ):
        """Subscribe to channel."""
        if self.use_redis and self.pubsub:
            await self.pubsub.subscribe(channel)

            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        stream_msg = StreamMessage(
                            message_type=MessageType(data["type"]),
                            timestamp=datetime.fromisoformat(data["timestamp"]),
                            data=data["data"],
                            message_id=data.get("id"),
                            sequence=data.get("sequence")
                        )
                        await callback(stream_msg)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        else:
            # Local subscription
            async with self.local_lock:
                if channel not in self.local_subscribers:
                    self.local_subscribers[channel] = set()
                self.local_subscribers[channel].add(callback)

    async def unsubscribe(self, channel: str, callback: Callable):
        """Unsubscribe from channel."""
        if self.use_redis and self.pubsub:
            await self.pubsub.unsubscribe(channel)
        else:
            async with self.local_lock:
                if channel in self.local_subscribers:
                    self.local_subscribers[channel].discard(callback)


class WebSocketManager:
    """
    Manages WebSocket connections with rate limiting and reconnection.

    Features:
    - Per-client rate limiting
    - Message buffering for backpressure
    - Reconnection state management
    - Heartbeat monitoring
    """

    def __init__(
        self,
        rate_limit: int = 100,
        heartbeat_interval: int = 30
    ):
        self.active_connections: Dict[str, WebSocket] = {}
        self.client_states: Dict[str, ClientState] = {}
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.rate_limit = rate_limit
        self.heartbeat_interval = heartbeat_interval
        self.sequence_counter = 0
        self.lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        client_id: Optional[str] = None
    ) -> str:
        """
        Accept WebSocket connection and register client.

        Returns client_id for tracking.
        """
        await websocket.accept()

        if client_id is None:
            client_id = str(uuid.uuid4())

        async with self.lock:
            self.active_connections[client_id] = websocket
            self.rate_limiters[client_id] = RateLimiter(
                rate=self.rate_limit,
                burst=self.rate_limit * 2
            )

            # Restore or create client state
            if client_id in self.client_states:
                state = self.client_states[client_id]
                state.last_heartbeat = datetime.now()
            else:
                self.client_states[client_id] = ClientState(client_id=client_id)

        # Send buffered messages outside the lock to avoid deadlock
        if client_id in self.client_states:
            await self._send_buffered_messages(client_id)

        logger.info(f"WebSocket client {client_id} connected")
        return client_id

    async def disconnect(self, client_id: str):
        """Disconnect client and cleanup (preserving state for reconnection)."""
        async with self.lock:
            if client_id in self.active_connections:
                del self.active_connections[client_id]
            if client_id in self.rate_limiters:
                del self.rate_limiters[client_id]

        logger.info(f"WebSocket client {client_id} disconnected")

    async def send_message(
        self,
        client_id: str,
        message: StreamMessage,
        respect_rate_limit: bool = True
    ):
        """Send message to specific client with rate limiting."""
        if client_id not in self.active_connections:
            # Buffer message for reconnection
            if client_id in self.client_states:
                self.client_states[client_id].message_buffer.append(message)
            return

        # Check rate limit
        if respect_rate_limit:
            rate_limiter = self.rate_limiters.get(client_id)
            if rate_limiter and not await rate_limiter.acquire():
                # Backpressure - buffer message
                if client_id in self.client_states:
                    self.client_states[client_id].message_buffer.append(message)
                return

        # Add sequence number
        async with self.lock:
            self.sequence_counter += 1
            message.sequence = self.sequence_counter

        # Send message
        websocket = self.active_connections[client_id]
        try:
            await websocket.send_json(message.to_dict())

            # Update client state
            if client_id in self.client_states:
                state = self.client_states[client_id]
                state.last_sequence = message.sequence
                state.last_message_id = message.message_id

        except Exception as e:
            logger.error(f"Error sending message to {client_id}: {e}")
            await self.disconnect(client_id)

    async def broadcast(
        self,
        message: StreamMessage,
        filter_fn: Optional[Callable[[ClientState], bool]] = None
    ):
        """Broadcast message to all connected clients."""
        for client_id in list(self.active_connections.keys()):
            # Apply filter if provided
            if filter_fn:
                state = self.client_states.get(client_id)
                if state and not filter_fn(state):
                    continue

            await self.send_message(client_id, message)

    async def _send_buffered_messages(self, client_id: str):
        """Send buffered messages to reconnected client."""
        state = self.client_states.get(client_id)
        if not state:
            return

        while state.message_buffer:
            message = state.message_buffer.popleft()
            await self.send_message(
                client_id,
                message,
                respect_rate_limit=False  # Send all buffered messages
            )

    async def heartbeat_loop(self):
        """Send periodic heartbeats to all clients."""
        while True:
            await asyncio.sleep(self.heartbeat_interval)

            heartbeat = StreamMessage(
                message_type=MessageType.HEARTBEAT,
                timestamp=datetime.now(),
                data={"server_time": datetime.now().isoformat()}
            )

            await self.broadcast(heartbeat)

    def get_client_state(self, client_id: str) -> Optional[ClientState]:
        """Get client state for reconnection."""
        return self.client_states.get(client_id)

    def cleanup_stale_states(self, max_age_hours: int = 24):
        """Remove client states older than max_age_hours."""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)

        to_remove = [
            client_id
            for client_id, state in self.client_states.items()
            if state.last_heartbeat < cutoff
        ]

        for client_id in to_remove:
            del self.client_states[client_id]
            logger.info(f"Cleaned up stale state for {client_id}")


class SSEManager:
    """
    Server-Sent Events manager for one-way streaming.

    Features:
    - Event streaming with automatic reconnection
    - Last-Event-ID support for reconnection
    - Rate limiting and backpressure
    """

    def __init__(self, rate_limit: int = 100):
        self.rate_limit = rate_limit
        self.client_states: Dict[str, ClientState] = {}
        self.sequence_counter = 0
        self.lock = asyncio.Lock()

    async def event_stream(
        self,
        client_id: str,
        last_event_id: Optional[str] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Generate SSE event stream.

        Yields events in SSE format.
        """
        rate_limiter = RateLimiter(
            rate=self.rate_limit,
            burst=self.rate_limit * 2
        )

        # Create or restore client state
        async with self.lock:
            if client_id not in self.client_states:
                self.client_states[client_id] = ClientState(client_id=client_id)

            state = self.client_states[client_id]

        # Send confirmation
        confirm_msg = StreamMessage(
            message_type=MessageType.SUBSCRIPTION_CONFIRMED,
            timestamp=datetime.now(),
            data={"client_id": client_id, "last_event_id": last_event_id}
        )

        yield {
            "id": confirm_msg.message_id,
            "event": confirm_msg.message_type.value,
            "data": json.dumps(confirm_msg.data)
        }

        # Stream messages
        try:
            while True:
                # Check for buffered messages
                if state.message_buffer:
                    message = state.message_buffer.popleft()
                else:
                    # Wait for new messages (would integrate with pub/sub)
                    await asyncio.sleep(0.1)
                    continue

                # Rate limiting
                await rate_limiter.wait()

                # Add sequence
                async with self.lock:
                    self.sequence_counter += 1
                    message.sequence = self.sequence_counter

                # Yield SSE formatted event
                yield {
                    "id": message.message_id,
                    "event": message.message_type.value,
                    "data": json.dumps(message.data),
                    "retry": 5000  # Retry after 5 seconds
                }

                # Update state
                state.last_sequence = message.sequence
                state.last_message_id = message.message_id
                state.last_heartbeat = datetime.now()

        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for {client_id}")

    async def publish_to_client(self, client_id: str, message: StreamMessage):
        """Add message to client buffer for SSE delivery."""
        if client_id in self.client_states:
            self.client_states[client_id].message_buffer.append(message)


class RealtimeStreamManager:
    """
    Unified real-time streaming manager.

    Coordinates WebSocket, SSE, and Pub/Sub for metric streaming.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        use_redis: bool = False,
        ws_rate_limit: int = 100,
        sse_rate_limit: int = 100
    ):
        self.broker = MessageBroker(redis_url, use_redis)
        self.ws_manager = WebSocketManager(rate_limit=ws_rate_limit)
        self.sse_manager = SSEManager(rate_limit=sse_rate_limit)

    async def start(self):
        """Start the streaming manager."""
        await self.broker.connect()

        # Start heartbeat loop
        self.heartbeat_task = asyncio.create_task(self.ws_manager.heartbeat_loop())

        logger.info("Realtime stream manager started")

    async def stop(self):
        """Stop the streaming manager."""
        # Cancel heartbeat loop
        if hasattr(self, 'heartbeat_task'):
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

        await self.broker.disconnect()
        logger.info("Realtime stream manager stopped")

    async def publish_metric_update(
        self,
        metric_name: str,
        resource: Dict[str, Any],
        value: float,
        timestamp: datetime
    ):
        """Publish metric update to all streams."""
        message = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=timestamp,
            data={
                "metric_name": metric_name,
                "resource": resource,
                "value": value,
                "timestamp": timestamp.isoformat()
            }
        )

        # Broadcast via pub/sub
        await self.broker.publish(f"metrics.{metric_name}", message)

        # Broadcast to WebSocket clients
        def filter_clients(state: ClientState) -> bool:
            if not state.subscription:
                return True
            if state.subscription.metric_names:
                return metric_name in state.subscription.metric_names
            return True

        await self.ws_manager.broadcast(message, filter_fn=filter_clients)

    async def publish_alert(
        self,
        alert_id: str,
        severity: str,
        metric_name: str,
        resource: Dict[str, Any],
        message_text: str
    ):
        """Publish alert to all streams."""
        message = StreamMessage(
            message_type=MessageType.ALERT,
            timestamp=datetime.now(),
            data={
                "alert_id": alert_id,
                "severity": severity,
                "metric_name": metric_name,
                "resource": resource,
                "message": message_text
            }
        )

        # Broadcast to all channels
        await self.broker.publish("alerts", message)
        await self.ws_manager.broadcast(message)


# Convenience functions for FastAPI integration

def create_stream_manager(
    redis_url: str = "redis://localhost:6379",
    use_redis: bool = False
) -> RealtimeStreamManager:
    """Create and initialize stream manager."""
    return RealtimeStreamManager(redis_url, use_redis)


async def websocket_endpoint(
    websocket: WebSocket,
    manager: RealtimeStreamManager,
    client_id: Optional[str] = None
):
    """
    WebSocket endpoint handler.

    Usage with FastAPI:
    ```python
    @app.websocket("/ws")
    async def websocket_route(websocket: WebSocket):
        await websocket_endpoint(websocket, stream_manager)
    ```
    """
    client_id = await manager.ws_manager.connect(websocket, client_id)

    try:
        while True:
            # Receive messages from client (subscription updates, etc.)
            data = await websocket.receive_json()

            # Handle subscription updates
            if data.get("action") == "subscribe":
                metric_names = data.get("metrics", [])
                # Update client subscription
                state = manager.ws_manager.get_client_state(client_id)
                if state:
                    state.subscription = ClientSubscription(
                        client_id=client_id,
                        metric_names=metric_names
                    )

    except WebSocketDisconnect:
        await manager.ws_manager.disconnect(client_id)


async def sse_endpoint(
    client_id: str,
    manager: RealtimeStreamManager,
    last_event_id: Optional[str] = None
) -> EventSourceResponse:
    """
    SSE endpoint handler.

    Usage with FastAPI:
    ```python
    @app.get("/sse")
    async def sse_route(client_id: str, last_event_id: str = None):
        return await sse_endpoint(client_id, stream_manager, last_event_id)
    ```
    """
    return EventSourceResponse(
        manager.sse_manager.event_stream(client_id, last_event_id)
    )
