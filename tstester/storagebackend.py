from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """The base class / interface for a time series storage backend."""

    def __init__(self, verbose, descr):
        self._verbose = verbose
        self._descr = descr

    def descr(self):
        """Return description of storage backend."""
        return self._descr

    @abstractmethod
    def reset(self, tss):
        """Replace any existing time series with tss.

        - tss is a list of TimeSeries objects
        """

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

        - tss is a list of (station_id, param_id) tuples.

        Returns two lists: times and obs (subject to the same restrictions as in set_obs())
        """

    @abstractmethod
    def get_obs_all(self, from_time, to_time):
        """Get all observations in time range [from_time, to_time> in all time series.

        Returns a list of (ts_id, times, obs) tuples, one per time series, where
        times and obs are lists subject to the same restrictions as in set_obs().
        """

    @abstractmethod
    def get_station_param(self, ts_id):
        """Get the station- and param ID that corresponds to ts_id

        Returns station_id, param_id.
        """

    @abstractmethod
    def get_tss_in_circle(self, lat, lon, radius):
        """Get time series within a circle.

        Returns a list of (station_id, param_id) tuples.
        """

    # TODO: add more methods:
    # get_tss_all()
    # get_tss_in_polygon()
    # get_tss_for_station()
    # get_tss_for_param()
    # get_tss_for_station_param()
    # ...
