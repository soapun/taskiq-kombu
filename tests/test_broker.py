import asyncio
from unittest.mock import MagicMock
import uuid
import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq_kombu.broker import KombuBroker
from taskiq.utils import maybe_awaitable
from kombu.message import Message
import threading


async def get_first_task(kombu_broker: KombuBroker) -> AckableMessage:  # type: ignore
    async for message in kombu_broker.listen():
        return message


MESSAGE = BrokerMessage(
    task_id=uuid.uuid4().hex,
    task_name=uuid.uuid4().hex,
    message=b"my_msg",
    labels={
        "label1": "val1",
    },
)

TIMEOUT = 1


async def test_kick_success(kombu_broker):
    await kombu_broker.kick(MESSAGE)
    message = await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)
    assert message.data == MESSAGE.message
    await maybe_awaitable(message.ack())


async def test_listen_empty_queue(kombu_broker):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)


async def test_listen_ack(kombu_broker):
    mock = MagicMock(Message)
    mock.body = b""
    await kombu_broker._message_queue.put(mock)
    message = await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)
    await message.ack()
    mock.ack.assert_called_once()


async def test_listen_exception(kombu_broker):
    mock = MagicMock(Message)
    # AckableMessage raises ValidationError

    await kombu_broker._message_queue.put(mock)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)

    1 == 1


async def test_listen_multiple(kombu_broker):
    await kombu_broker.kick(MESSAGE)
    await kombu_broker.kick(MESSAGE)

    result = []
    i = 0
    async for message in kombu_broker.listen():
        await message.ack()
        result.append(message)

        i += 1
        if i == 2:
            break

    assert result[0].data == MESSAGE.message
    assert result[1].data == MESSAGE.message


async def test_shutdown(kombu_broker):
    kombu_broker._message_queue.put_nowait({})
    await kombu_broker.shutdown()

    assert kombu_broker._shutdown_event.is_set()
    assert not kombu_broker._message_queue.empty()
    assert not kombu_broker._consumer_thread


async def test_startup_after_shutdown(kombu_broker):
    kombu_broker._message_queue.put_nowait({})
    await kombu_broker.shutdown()
    await kombu_broker.startup()

    assert not kombu_broker._shutdown_event.is_set()
    assert kombu_broker._message_queue.empty()
    assert not kombu_broker._consumer_thread


async def test_shutdown_after_listen(kombu_broker):
    await kombu_broker.kick(MESSAGE)
    message = await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)
    await message.ack()  # type: ignore
    await kombu_broker.shutdown()

    assert kombu_broker._shutdown_event.is_set()
    assert not kombu_broker._consumer_thread


async def test_listen_before_startup(kombu_broker):
    await kombu_broker.shutdown()
    with pytest.raises(ValueError, match="Call startup"):
        await asyncio.wait_for(get_first_task(kombu_broker), timeout=TIMEOUT)


class DummyConsumer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def test_consumer_worker_timeout(monkeypatch):
    broker = KombuBroker(
        connection=MagicMock(),
        queues=[MagicMock()],
    )
    broker._shutdown_event = threading.Event()
    broker._shutdown_event.clear()

    def dummy_drain_events(*args, **kwargs):
        broker._shutdown_event.set()
        raise TimeoutError()

    monkeypatch.setattr(broker, "_on_message", lambda body, msg: None)
    monkeypatch.setattr(
        "taskiq_kombu.broker.Consumer", lambda *a, **kw: DummyConsumer()
    )
    broker._read_connection.drain_events = dummy_drain_events

    # Should not raise
    broker._consumer_worker()


def test_consumer_worker_exception(monkeypatch, caplog):
    broker = KombuBroker(
        connection=MagicMock(),
        queues=[MagicMock()],
    )
    broker._shutdown_event.clear()

    def dummy_drain_events(*args, **kwargs):
        broker._shutdown_event.set()
        raise RuntimeError("fail")

    monkeypatch.setattr(broker, "_on_message", lambda body, msg: None)
    monkeypatch.setattr(
        "taskiq_kombu.broker.Consumer", lambda *args, **kwargs: DummyConsumer()
    )
    broker._read_connection.drain_events = dummy_drain_events
    broker._consumer_worker()
    assert "Error consuming from queue" in caplog.text
