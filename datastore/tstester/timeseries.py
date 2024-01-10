import math


class TimeSeries:
    """Represents a time series of observations.

    The time series is uniquely identified by the (station_id, param_id) combo.
    """

    def __init__(self, verbose, station_id, lat, lon, param_id, time_res, other_mdata, obs_mdata):
        self._verbose = verbose
        self._station_id = station_id

        self._lat = lat
        self._lon = lon  # horizontal location of the station

        self._param_id = param_id
        if time_res < 1:
            raise Exception("non-positive time resolution not allowed: {}".format(time_res))
        self._time_res = time_res  # time resolution, i.e. seconds between observations
        self._other_mdata = other_mdata  # other metadata (e.g. quality of sensor)

        self._obs_mdata = obs_mdata  # per obs metadata (e.g. quality of specific obs value)
        # TODO: obs_mdata should really just define the per obs metadata *fields*, not
        # the actual values; the latter should be generated (typically randomized) by
        # create_observations()

        self._sin_wave_period = 86400  # sin wave period in secs (a 24H cycle)

    def station_id(self):
        return self._station_id

    def param_id(self):
        return self._param_id

    def lat(self):
        return self._lat

    def lon(self):
        return self._lon

    def other_mdata(self):
        return self._other_mdata

    def create_observations(self, t0, t1):
        """Create observations in timestamp range [t0, t1>.

        The observations are formed by sampling an underlying sine wave with frequency
        1/self._sine_wave_period_secs, starting at UNIX epoch. The wave is sampled every
        self._time_res second.

        Returns two arrays:
            [obs time 1, obs time 2, ..., obs time n]
            [obs value 1, obs value 2, ..., obs value n]
        TODO: also return per obs metadata as defined by self._obs_mdata
        """

        if t0 >= t1:
            raise Exception("invalid obs time range: [{}, {}]".format(t0, t1))

        times = []
        values = []
        f = (2 * math.pi) / self._sin_wave_period
        for t in range(t0, t1, self._time_res):
            times.append(t)
            values.append(math.sin((t % self._sin_wave_period) * f))

        return times, values
