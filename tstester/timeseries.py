import math


class TimeSeries:
    """Represents a time series of observations.

    The time series is uniquely identified by either:
      1: the (station_name, param_name) combo, or
      2: the (lat, lon) combo.
    """

    def __init__(
            self, verbose, station_name, lat, lon, param_name, time_res, ts_mdata, obs_mdata):
        self._verbose = verbose
        self._station_name = station_name

        self._lat = lat
        self._lon = lon  # horizontal location of the station

        self._param_name = param_name
        if time_res < 1:
            raise Exception('non-positive time resolution not allowed: {}'.format(time_res))
        self._time_res = time_res  # time resolution, i.e. seconds between observations
        self._ts_mdata = ts_mdata  # overall metadata for the time series (e.g. quality of sensor)
        self._obs_mdata = obs_mdata  # per-obs metadata (e.g. quality of specific obs value)

    def create_observations(self, t0, t1):
        """Create observations in timestamp range [t0, t1>.

        The observations are formed by sampling a sine wave with frequency 1/(t1-t0)
        (i.e. the full range [t0, t1> represents a single cycle), and with a resolution of
        self._time_res (i.e. self._time_res == 60 would produce a sample every 60th second,
        and so on).

        Returns two arrays:
            [time 1, time 2, ..., time n]
            [obs 1, obs 2, ..., obs n]
        """

        if t0 >= t1:
            raise Exception('invalid obs time range: [{}, {}]'.format(t0, t1))

        times = []
        obs = []
        f = 1.0 / (t1 - t0)
        for t in range(t0, t1, self._time_res):
            times.append(t)
            obs.append(math.sin((t - t0) * f))

        return times, obs
