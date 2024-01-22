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
    def set_obs(self, ts, times, values):
        """Replace any existing observations in time series ts with times/values.

        - times are assumed to be a list of
          unique UNIX timestamps (secs since 1970-01-01T00:00:00Z)
          in strictly increasing order, but not necessarily uniformly distributed
        - values are assumed to be a list of floats
        - len(times) and len(values) are assumed to be equal
        """

    @abstractmethod
    def add_obs(self, ts, times, values, oldest_time):
        """Adds observations to time series ts.

        - times/values: same as in set_obs()
        - if oldest_time is not None, observations older than this time are removed
          from the storage

        Observations at already existing times will be replaced.
        """

    @abstractmethod
    def get_obs(self, ts_ids, from_time, to_time):
        """Get observations in time range [from_time, to_time> from time series in ts_ids.

        - ts_ids is a list of time series IDs.

        Returns two lists: times and values (subject to the same restrictions as in set_obs())
        """

    @abstractmethod
    def get_obs_all(self, from_time, to_time):
        """Get all observations in time range [from_time, to_time> in all time series.

        Returns a list of (ts_id, times, values) tuples, one per time series, where
        times and values are lists subject to the same restrictions as in set_obs().
        """

    @abstractmethod
    def get_station_and_param(self, ts_id):
        """Get the station- and param ID that corresponds to ts_id

        Returns station_id, param_id.
        """

    @abstractmethod
    def get_ts_ids_all(self):
        """Get all time series.

        Returns a list of (station_id, param_id) tuples.
        """

    @abstractmethod
    def get_ts_ids_in_circle(self, lat, lon, radius):
        """Get time series within a circle.

        - lat,lon is centre point in degrees
        - radius is kilometers along earth surface (valid range: [0, ?])

        Returns a list of all time series IDs inside the circle.
        """

    # TODO: add more methods:
    # get_ts_ids_in_polygon()
    # get_ts_ids_for_station()
    # get_ts_ids_for_param()
    # get_ts_ids_for_station_param()
    # ...
