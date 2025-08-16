import importlib.metadata

__version__ = importlib.metadata.version(__name__)

from taskiq_kombu.broker import KombuBroker

__all__ = ["KombuBroker"]
