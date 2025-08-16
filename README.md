# taskiq-kombu

taskiq-kombu is a plugin for taskiq that adds a new broker based on [kombu](https://github.com/celery/kombu).

The broker makes use of kombu `Consumer` and `Producer` so you can use any kombu transport as taskiq broker.

[![CI](https://github.com/soapun/taskiq-kombu/actions/workflows/ci.yml/badge.svg)](https://github.com/soapun/taskiq-kombu/actions/workflows/ci.yml) [![PyPI](https://badge.fury.io/py/taskiq-kombu.svg)](https://badge.fury.io/py/taskiq-kombu) [![Python](https://img.shields.io/pypi/pyversions/taskiq-kombu.svg)](https://pypi.org/project/taskiq-kombu/)

[![License](https://img.shields.io/pypi/l/taskiq-kombu.svg)](https://github.com/soapun/taskiq-kombu/blob/main/LICENSE) [![MyPy](https://img.shields.io/badge/type_checked-mypy-informational.svg)](https://mypy.readthedocs.io/en/stable/introduction.html) [![Ruff](https://img.shields.io/badge/style-ruff-blue?logo=ruff&logoColor=white)](https://github.com/astral-sh/ruff) [![codecov](https://codecov.io/github/soapun/taskiq-kombu/graph/badge.svg?token=RCMKRL0SFC)](https://codecov.io/github/soapun/taskiq-kombu)


---

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq
```

This project can be installed using pip:

```bash
pip install taskiq-kombu
```

Or using uv:

```
uv add taskiq-kombu
```

## Usage

An example with the broker

```python
# example.py
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
```

### Run example

**shell 1: start a worker**

```sh
$ taskiq worker example:broker
```

**shell 2: run the example script**

```sh
$ python example.py
```
