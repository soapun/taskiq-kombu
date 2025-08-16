import asyncio
from kombu import Connection, Queue

from taskiq_kombu import KombuBroker

broker = KombuBroker(
    connection=Connection("sqla+sqlite:///tasks.db"),
    queues=[
        Queue("my_queue", exchange="", routing_key="my_queue"),
    ],
)


@broker.task("my_task")
async def my_task(a: int, b: int) -> None:
    print("AB", a + b)


async def main():
    await broker.startup()

    await my_task.kiq(1, 2)

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
