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
    def set_obs(self, ts, times, obs):
        """Replace any existing observations in time series ts with times/obs.

        - times are assumed to be a list of unique UNIX timestamps (secs since 1970-01-01T00:00:00Z)
          in strictly increasing order, but not necessesarily uniformly distributed
        - obs are assumed to be a list of floats
        - len(times) and len(obs) are assumed to be equal
        """

    @abstractmethod
    def add_obs(self, ts, times, obs):
        """Adds observations to time series ts.

        - times/obs: same as in set_obs()

        Observations at alredy existing times will be replaced.
        """

    @abstractmethod
    def get_obs(self, tss, from_time, to_time):
        """Get observations in time range [from_time, to_time> from time series in tss.

        - tss is a list of (station_name, param_name) tuples.

        Returns two lists: times and obs (subject to the same restrictions as in set_obs())
        """

    @abstractmethod
    def get_tss_in_circle(self, lat, lon, radius):
        """Get time series within a circle.

        Returns a list of (station_name, param_name) tuples.
        """

    # TODO: add more methods to find time series:
    # get_tss_in_polygon()
    # get_all_tss()
    # get_tss_for_station()
    # get_tss_for_param()
    # get_tss_for_station_param()
    # ...
