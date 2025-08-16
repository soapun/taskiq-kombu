from copy import copy
from typing import Callable, Optional, TypeVar, AsyncGenerator
import asyncio
import threading
import logging


from kombu import Connection, Queue, Consumer, Producer, Message
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")

logger = logging.getLogger("taskiq.kombu_broker")


class KombuBroker(AsyncBroker):
    def __init__(
        self,
        connection: Connection,
        queues: list[Queue],
        poll_interval: int = 1,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ):
        """
        Taskiq broker using Kombu with SQLAlchemy transport.

        :param connection: Pre-configured Kombu Connection instance
        :param queues: List of kombu Queue objects to produce to and consume from
        :parma poll_interval: The time interval, in seconds, between `drain_events` from kombu connection
        :param result_backend: Optional result backend
        :param task_id_generator: Optional custom task ID generator
        """
        super().__init__(result_backend, task_id_generator)
        self._read_connection = connection
        self._write_connection = copy(connection)
        self.queues = queues
        self.poll_interval = poll_interval
        self._shutdown_event = threading.Event()
        self._shutdown_event.set()
        self._message_queue: asyncio.Queue[Message] = asyncio.Queue()
        self._consumer_thread: Optional[threading.Thread] = None

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message to the queues.

        :param message: The message to send
        """

        with Producer(self._write_connection) as producer:
            for queue in self.queues:
                producer.publish(
                    body=message.message,
                    content_type="application/data",
                    content_encoding="binary",
                    exchange=queue.exchange,
                    routing_key=queue.routing_key,
                    declare=[queue],
                    headers={
                        "task_id": message.task_id,
                        "task_name": message.task_name,
                    },
                )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen for incoming messages from the queues.

        :yield: AckableMessage instances.
        """

        if self._shutdown_event.is_set():
            raise ValueError("Call startup before starting listening")

        if not self._consumer_thread:
            self._consumer_thread = threading.Thread(
                target=self._consumer_worker,
                daemon=True,
            )
            self._consumer_thread.start()

        while True:
            try:
                kombu_msg = await self._message_queue.get()

                async def ack() -> None:
                    await asyncio.get_event_loop().run_in_executor(None, kombu_msg.ack)

                yield AckableMessage(
                    data=kombu_msg.body,
                    ack=ack,
                )
            except Exception as e:
                logger.exception(f"Error processing message: {e}")

    async def startup(self) -> None:
        """Startup the broker."""
        await super().startup()
        self._shutdown_event.clear()
        self._message_queue = asyncio.Queue()
        self._loop = asyncio.get_event_loop()

        if not self._read_connection.connected:
            self._read_connection.connect()

        if not self._write_connection.connected:
            self._write_connection.connect()

    async def shutdown(self) -> None:
        """Shutdown the broker."""
        await super().shutdown()

        self._shutdown_event.set()
        if self._consumer_thread:
            self._consumer_thread.join()
            self._is_running = False
            self._consumer_thread = None

        if self._read_connection.connected:
            self._read_connection.close()

        if self._write_connection.connected:
            self._write_connection.close()

    def _consumer_worker(self) -> None:
        """Run the Kombu consumer in a background thread."""
        while not self._shutdown_event.is_set():
            with Consumer(
                self._read_connection,
                self.queues,
                callbacks=[self._on_message],
                accept=["json"],
            ):
                while not self._shutdown_event.is_set():
                    try:
                        self._read_connection.drain_events(timeout=self.poll_interval)
                    except TimeoutError:
                        pass
                    except Exception as e:
                        logger.exception(f"Error consuming from queue: {e}")

    def _on_message(self, body: bytes, message: Message) -> None:
        """Callback for received messages."""
        asyncio.run_coroutine_threadsafe(self._message_queue.put(message), self._loop)
