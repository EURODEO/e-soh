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

        self._sin_wave_period = 86400  # sin wave period in secs (a 24H cycle)

    def create_observations(self, t0, t1):
        """Create observations in timestamp range [t0, t1>.

        The observations are formed by sampling an underlying sine wave with frequency
        1/self._sine_wave_period_secs, starting at UNIX epoch. The wave is sampled every
        self._time_res second.

        Returns two arrays:
            [time 1, time 2, ..., time n]
            [obs 1, obs 2, ..., obs n]
        """

        if t0 >= t1:
            raise Exception('invalid obs time range: [{}, {}]'.format(t0, t1))

        times = []
        obs = []
        f = (2 * math.pi) / self._sin_wave_period
        for t in range(t0, t1, self._time_res):
            times.append(t)
            obs.append(math.sin((t % self._sin_wave_period) * f))

        return times, obs
