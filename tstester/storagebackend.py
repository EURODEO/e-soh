from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """The base class / interface for a time series storage backend."""

    def __init__(self, verbose):
        self._verbose = verbose

    @abstractmethod
    def reset(self, tss):
        """Replace any existing time series with tss."""

    @abstractmethod
    def set_observations(self, ts, obs):
        """Replace any existing observations of time series ts with obs."""
