from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from prometheus_client import multiprocess
from prometheus_fastapi_instrumentator import Instrumentator


def add_metrics(app: FastAPI):
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)

    instrumentator = Instrumentator()
    instrumentator.instrument(app).expose(app, include_in_schema=False)
