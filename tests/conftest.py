from pathlib import Path
from typing import AsyncGenerator
import tempfile
import uuid


from kombu import Connection, Queue
import pytest

from taskiq_kombu.broker import KombuBroker


@pytest.fixture()
async def kombu_broker() -> AsyncGenerator[KombuBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    name = "test_queue"

    db_path = Path(tempfile.gettempdir()) / f"taskiq-kombu-{uuid.uuid4().hex[:8]}"

    broker = KombuBroker(
        connection=Connection(f"sqla+sqlite:///{db_path.absolute().__str__()}"),
        queues=[
            Queue(name, exchange="", routing_key=name),
        ],
    )
    await broker.startup()
    yield broker
    await broker.shutdown()
