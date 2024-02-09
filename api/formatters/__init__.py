import logging
from enum import Enum

from . import covjson

logger = logging.getLogger(__name__)


class Formats(str, Enum):
    covjson = "covjson"


def get_edr_formatters() -> dict:
    available_formatters = {"covjson": covjson.Covjson()}
    # Should also setup dict for alias discover

    return available_formatters
