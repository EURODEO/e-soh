from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """The base class / interface for a time series storage backend."""

    def __init__(self, verbose, name):
        self._verbose = verbose
        self._name = name

    def name(self):
        """Return name of storage backend."""
        return self._name

    @abstractmethod
    def reset(self, tss):
        """Replace any existing time series with tss."""

    @abstractmethod
    def set_observations(self, ts, times, obs):
        """Replace any existing observations of time series ts with times/obs.

        - times are assumed to be an array of UNIX timestamps (secs since 1970-01-01T00:00:00Z)
          in strictly increasing order (but not necessesarily uniformly distributed)
        - obs are assumed to be an array of floats
        - len(times) and len(obs) are assumed to be equal
        """
