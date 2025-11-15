"""
Test suite for real-time streaming system.

Tests WebSocket, SSE, pub/sub, rate limiting, and concurrent clients.
"""

import pytest
import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from collections import deque

from src.realtime import (
    StreamMessage,
    MessageType,
    ClientSubscription,
    ClientState,
    RateLimiter,
    MessageBroker,
    WebSocketManager,
    SSEManager,
    RealtimeStreamManager,
    StreamProtocol
)


@pytest.fixture
def sample_message():
    """Sample stream message."""
    return StreamMessage(
        message_type=MessageType.METRIC_UPDATE,
        timestamp=datetime.now(),
        data={
            "metric_name": "cpu.usage",
            "resource": {"host": "server-01"},
            "value": 75.5
        }
    )


@pytest.fixture
def client_subscription():
    """Sample client subscription."""
    return ClientSubscription(
        client_id="client-123",
        metric_names=["cpu.usage", "memory.usage"],
        min_interval_ms=100,
        max_buffer_size=1000
    )


class TestStreamMessage:
    """Test StreamMessage dataclass."""

    def test_message_creation(self):
        """Test creating a stream message."""
        msg = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            data={"value": 100}
        )

        assert msg.message_type == MessageType.METRIC_UPDATE
        assert msg.data["value"] == 100
        assert msg.message_id is not None  # Auto-generated

    def test_message_to_dict(self, sample_message):
        """Test converting message to dictionary."""
        result = sample_message.to_dict()

        assert result["type"] == "metric_update"
        assert "timestamp" in result
        assert result["data"]["metric_name"] == "cpu.usage"
        assert result["id"] == sample_message.message_id

    def test_message_to_json(self, sample_message):
        """Test converting message to JSON."""
        json_str = sample_message.to_json()
        data = json.loads(json_str)

        assert data["type"] == "metric_update"
        assert data["data"]["value"] == 75.5

    def test_message_with_sequence(self):
        """Test message with sequence number."""
        msg = StreamMessage(
            message_type=MessageType.HEARTBEAT,
            timestamp=datetime.now(),
            data={},
            sequence=42
        )

        assert msg.sequence == 42


class TestClientState:
    """Test ClientState for reconnection."""

    def test_client_state_creation(self):
        """Test creating client state."""
        state = ClientState(client_id="client-123")

        assert state.client_id == "client-123"
        assert state.last_sequence == 0
        assert isinstance(state.message_buffer, deque)
        assert state.connected_at is not None

    def test_message_buffer_limit(self):
        """Test message buffer size limit."""
        state = ClientState(client_id="client-123")

        # Add more than maxlen messages
        for i in range(1500):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            state.message_buffer.append(msg)

        # Should only keep last 1000
        assert len(state.message_buffer) == 1000
        # Oldest messages dropped
        assert state.message_buffer[0].data["seq"] == 500


class TestRateLimiter:
    """Test token bucket rate limiter."""

    @pytest.mark.asyncio
    async def test_acquire_tokens(self):
        """Test acquiring tokens."""
        limiter = RateLimiter(rate=100, burst=200)

        # Should have full burst initially
        assert await limiter.acquire(100)
        assert await limiter.acquire(100)

    @pytest.mark.asyncio
    async def test_rate_limit_exceeded(self):
        """Test rate limit exceeded."""
        limiter = RateLimiter(rate=10, burst=10)

        # Exhaust tokens
        assert await limiter.acquire(10)

        # Should fail immediately
        assert not await limiter.acquire(1)

    @pytest.mark.asyncio
    async def test_token_refill(self):
        """Test tokens refill over time."""
        limiter = RateLimiter(rate=100, burst=100)

        # Exhaust tokens
        await limiter.acquire(100)

        # Wait for refill
        await asyncio.sleep(0.5)  # 50 tokens should refill

        assert await limiter.acquire(40)

    @pytest.mark.asyncio
    async def test_wait_for_tokens(self):
        """Test waiting for tokens with backpressure."""
        limiter = RateLimiter(rate=100, burst=10)

        # Exhaust tokens
        await limiter.acquire(10)

        # Wait should succeed eventually
        start = asyncio.get_event_loop().time()
        await limiter.wait(5)
        elapsed = asyncio.get_event_loop().time() - start

        # Should have waited for tokens to refill
        assert elapsed > 0.01


class TestMessageBroker:
    """Test pub/sub message broker."""

    @pytest.mark.asyncio
    async def test_local_pubsub(self):
        """Test local pub/sub without Redis."""
        broker = MessageBroker(use_redis=False)
        await broker.connect()

        received_messages = []

        async def callback(msg: StreamMessage):
            received_messages.append(msg)

        # Subscribe
        subscribe_task = asyncio.create_task(
            broker.subscribe("test-channel", callback)
        )

        # Give subscription time to setup
        await asyncio.sleep(0.1)

        # Publish message
        test_msg = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"test": "value"}
        )
        await broker.publish("test-channel", test_msg)

        # Give time for delivery
        await asyncio.sleep(0.1)

        assert len(received_messages) == 1
        assert received_messages[0].data["test"] == "value"

        subscribe_task.cancel()

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self):
        """Test multiple subscribers receive messages."""
        broker = MessageBroker(use_redis=False)
        await broker.connect()

        received_1 = []
        received_2 = []

        async def callback1(msg):
            received_1.append(msg)

        async def callback2(msg):
            received_2.append(msg)

        # Subscribe both
        task1 = asyncio.create_task(broker.subscribe("channel", callback1))
        task2 = asyncio.create_task(broker.subscribe("channel", callback2))

        await asyncio.sleep(0.1)

        # Publish
        msg = StreamMessage(
            message_type=MessageType.ALERT,
            timestamp=datetime.now(),
            data={"alert": "test"}
        )
        await broker.publish("channel", msg)

        await asyncio.sleep(0.1)

        # Both should receive
        assert len(received_1) == 1
        assert len(received_2) == 1

        task1.cancel()
        task2.cancel()


class TestWebSocketManager:
    """Test WebSocket connection manager."""

    @pytest.mark.asyncio
    async def test_connect_client(self):
        """Test connecting a WebSocket client."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        assert client_id in manager.active_connections
        assert client_id in manager.client_states
        assert client_id in manager.rate_limiters
        mock_ws.accept.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_client(self):
        """Test disconnecting a client."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)
        await manager.disconnect(client_id)

        assert client_id not in manager.active_connections
        # State preserved for reconnection
        assert client_id in manager.client_states

    @pytest.mark.asyncio
    async def test_send_message(self):
        """Test sending message to client."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        message = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"value": 100}
        )

        await manager.send_message(client_id, message)

        # Should have sent JSON
        mock_ws.send_json.assert_called_once()
        call_args = mock_ws.send_json.call_args[0][0]
        assert call_args["type"] == "metric_update"
        assert "sequence" in call_args  # Sequence added

    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting prevents message flood."""
        manager = WebSocketManager(rate_limit=5)  # Very low rate
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        # Send many messages
        for i in range(20):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            await manager.send_message(client_id, msg)

        # Should have sent some, buffered rest
        sent_count = mock_ws.send_json.call_count
        state = manager.client_states[client_id]

        assert sent_count < 20  # Some were rate limited
        assert len(state.message_buffer) > 0  # Rest buffered

    @pytest.mark.asyncio
    async def test_broadcast(self):
        """Test broadcasting to all clients."""
        manager = WebSocketManager()
        mock_ws1 = AsyncMock()
        mock_ws2 = AsyncMock()

        client1 = await manager.connect(mock_ws1)
        client2 = await manager.connect(mock_ws2)

        message = StreamMessage(
            message_type=MessageType.HEARTBEAT,
            timestamp=datetime.now(),
            data={}
        )

        await manager.broadcast(message)

        # Both should receive
        mock_ws1.send_json.assert_called_once()
        mock_ws2.send_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_filtered_broadcast(self):
        """Test broadcasting with filter."""
        manager = WebSocketManager()
        mock_ws1 = AsyncMock()
        mock_ws2 = AsyncMock()

        client1 = await manager.connect(mock_ws1)
        client2 = await manager.connect(mock_ws2)

        # Set subscription for client1 only
        manager.client_states[client1].subscription = ClientSubscription(
            client_id=client1,
            metric_names=["cpu.usage"]
        )

        message = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"metric_name": "cpu.usage"}
        )

        # Filter: only clients with subscription
        def filter_fn(state: ClientState) -> bool:
            return state.subscription is not None

        await manager.broadcast(message, filter_fn=filter_fn)

        # Only client1 should receive
        mock_ws1.send_json.assert_called_once()
        mock_ws2.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconnection_buffer(self):
        """Test message buffering for reconnection."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)
        await manager.disconnect(client_id)

        # Send messages while disconnected
        for i in range(5):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            await manager.send_message(client_id, msg)

        # Should be buffered
        state = manager.client_states[client_id]
        assert len(state.message_buffer) == 5

        # Reconnect
        await manager.connect(mock_ws, client_id)

        # Buffered messages should be sent
        assert mock_ws.send_json.call_count == 5


class TestSSEManager:
    """Test Server-Sent Events manager."""

    @pytest.mark.asyncio
    async def test_event_stream_creation(self):
        """Test creating SSE event stream."""
        manager = SSEManager()

        events = []
        async for event in manager.event_stream("client-123"):
            events.append(event)
            if len(events) >= 1:  # Get confirmation
                break

        # Should receive subscription confirmation
        assert len(events) == 1
        assert events[0]["event"] == "subscription_confirmed"

    @pytest.mark.asyncio
    async def test_sse_message_delivery(self):
        """Test delivering messages via SSE."""
        manager = SSEManager()

        # Add message to buffer
        msg = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"value": 100}
        )
        await manager.publish_to_client("client-123", msg)

        # Consume stream
        events = []
        async for event in manager.event_stream("client-123"):
            events.append(event)
            if len(events) >= 2:  # Confirmation + message
                break

        assert len(events) == 2
        assert events[1]["event"] == "metric_update"

    @pytest.mark.asyncio
    async def test_sse_reconnection(self):
        """Test SSE reconnection with last_event_id."""
        manager = SSEManager()

        # Create stream with last_event_id
        events = []
        async for event in manager.event_stream("client-123", last_event_id="evt-123"):
            events.append(event)
            if len(events) >= 1:
                break

        # Should include last_event_id in confirmation
        data = json.loads(events[0]["data"])
        assert data["last_event_id"] == "evt-123"


class TestRealtimeStreamManager:
    """Test unified realtime stream manager."""

    @pytest.mark.asyncio
    async def test_manager_start_stop(self):
        """Test starting and stopping manager."""
        manager = RealtimeStreamManager(use_redis=False)

        await manager.start()
        assert manager.broker is not None

        await manager.stop()

    @pytest.mark.asyncio
    async def test_publish_metric_update(self):
        """Test publishing metric update."""
        manager = RealtimeStreamManager(use_redis=False)
        await manager.start()

        mock_ws = AsyncMock()
        client_id = await manager.ws_manager.connect(mock_ws)

        # Publish metric
        await manager.publish_metric_update(
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            value=75.5,
            timestamp=datetime.now()
        )

        # Give time for broadcast
        await asyncio.sleep(0.1)

        # Client should receive
        mock_ws.send_json.assert_called()

        await manager.stop()

    @pytest.mark.asyncio
    async def test_publish_alert(self):
        """Test publishing alert."""
        manager = RealtimeStreamManager(use_redis=False)
        await manager.start()

        mock_ws = AsyncMock()
        await manager.ws_manager.connect(mock_ws)

        await manager.publish_alert(
            alert_id="alert-123",
            severity="critical",
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            message_text="CPU high"
        )

        await asyncio.sleep(0.1)

        # Should broadcast to all clients
        mock_ws.send_json.assert_called()

        await manager.stop()


class TestConcurrentClients:
    """Test concurrent client handling."""

    @pytest.mark.asyncio
    async def test_multiple_concurrent_connections(self):
        """Test handling multiple concurrent WebSocket connections."""
        manager = WebSocketManager()

        # Connect 10 clients
        clients = []
        for i in range(10):
            mock_ws = AsyncMock()
            client_id = await manager.connect(mock_ws)
            clients.append((client_id, mock_ws))

        assert len(manager.active_connections) == 10

        # Broadcast message
        message = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"value": 100}
        )

        await manager.broadcast(message)

        # All should receive
        for _, mock_ws in clients:
            mock_ws.send_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_concurrent_message_delivery(self):
        """Test delivering messages to concurrent clients."""
        manager = WebSocketManager()

        # Connect clients
        clients = []
        for i in range(5):
            mock_ws = AsyncMock()
            client_id = await manager.connect(mock_ws)
            clients.append((client_id, mock_ws))

        # Send concurrent messages
        async def send_messages(client_id, count):
            for i in range(count):
                msg = StreamMessage(
                    message_type=MessageType.METRIC_UPDATE,
                    timestamp=datetime.now(),
                    data={"seq": i}
                )
                await manager.send_message(client_id, msg)

        # Send 10 messages to each client concurrently
        tasks = [send_messages(cid, 10) for cid, _ in clients]
        await asyncio.gather(*tasks)

        # Each client should receive their messages
        for _, mock_ws in clients:
            assert mock_ws.send_json.call_count > 0

    @pytest.mark.asyncio
    async def test_message_ordering(self):
        """Test message ordering is preserved."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        # Send sequential messages
        for i in range(10):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            await manager.send_message(client_id, msg, respect_rate_limit=False)

        # Check sequence numbers are increasing
        calls = mock_ws.send_json.call_args_list
        sequences = [call[0][0]["sequence"] for call in calls]

        assert sequences == sorted(sequences)  # Should be ordered

    @pytest.mark.asyncio
    async def test_high_throughput(self):
        """Test handling high message throughput."""
        manager = WebSocketManager(rate_limit=1000)
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        # Send 100 messages rapidly
        start = asyncio.get_event_loop().time()

        for i in range(100):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            await manager.send_message(client_id, msg)

        elapsed = asyncio.get_event_loop().time() - start

        # Should handle quickly (less than 1 second with rate limiting)
        assert elapsed < 2.0

        # Most should be delivered or buffered
        sent = mock_ws.send_json.call_count
        state = manager.client_states[client_id]
        buffered = len(state.message_buffer)

        assert sent + buffered == 100


class TestBackpressure:
    """Test backpressure handling."""

    @pytest.mark.asyncio
    async def test_buffer_overflow_prevention(self):
        """Test preventing buffer overflow."""
        manager = WebSocketManager(rate_limit=1)  # Very low rate
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)

        # Send way more messages than buffer can hold
        for i in range(2000):
            msg = StreamMessage(
                message_type=MessageType.METRIC_UPDATE,
                timestamp=datetime.now(),
                data={"seq": i}
            )
            await manager.send_message(client_id, msg)

        # Buffer should not exceed max size
        state = manager.client_states[client_id]
        assert len(state.message_buffer) <= 1000

    @pytest.mark.asyncio
    async def test_rate_limiter_backpressure(self):
        """Test rate limiter applies backpressure."""
        limiter = RateLimiter(rate=10, burst=10)

        # Exhaust tokens
        await limiter.acquire(10)

        # Try to acquire more
        acquired = await limiter.acquire(5)

        # Should fail (backpressure)
        assert not acquired


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_send_to_disconnected_client(self):
        """Test sending to disconnected client."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()

        client_id = await manager.connect(mock_ws)
        await manager.disconnect(client_id)

        # Send message to disconnected client
        msg = StreamMessage(
            message_type=MessageType.METRIC_UPDATE,
            timestamp=datetime.now(),
            data={"test": "value"}
        )

        await manager.send_message(client_id, msg)

        # Should buffer instead of crash
        state = manager.client_states[client_id]
        assert len(state.message_buffer) == 1

    @pytest.mark.asyncio
    async def test_websocket_send_error(self):
        """Test handling WebSocket send error."""
        manager = WebSocketManager()
        mock_ws = AsyncMock()
        mock_ws.send_json.side_effect = Exception("Connection closed")

        client_id = await manager.connect(mock_ws)

        msg = StreamMessage(
            message_type=MessageType.HEARTBEAT,
            timestamp=datetime.now(),
            data={}
        )

        # Should handle error gracefully
        await manager.send_message(client_id, msg)

        # Client should be disconnected
        assert client_id not in manager.active_connections

    @pytest.mark.asyncio
    async def test_cleanup_stale_states(self):
        """Test cleaning up old client states."""
        manager = WebSocketManager()

        # Create old state
        old_client = "old-client"
        manager.client_states[old_client] = ClientState(client_id=old_client)
        manager.client_states[old_client].last_heartbeat = (
            datetime.now() - timedelta(hours=25)
        )

        # Create fresh state
        fresh_client = "fresh-client"
        manager.client_states[fresh_client] = ClientState(client_id=fresh_client)

        # Cleanup
        manager.cleanup_stale_states(max_age_hours=24)

        # Old should be removed
        assert old_client not in manager.client_states
        # Fresh should remain
        assert fresh_client in manager.client_states
