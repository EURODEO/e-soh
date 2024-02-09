import logging
from enum import Enum

from . import covjson

logger = logging.getLogger(__name__)


class Formats(str, Enum):
    covjson = "CoverageJSON"  # According to EDR spec


formatters = {"CoverageJSON": covjson.convert_to_covjson}
