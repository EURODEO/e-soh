import common
import random
from postgis import PostGIS
from netcdf import NetCDF
from timeseries import TimeSeries
from abc import ABC, abstractmethod


class TestBase(ABC):
    def __init__(self, verbose, config, storage_backends):
        self._verbose = verbose
        self._config = config
        self._storage_backends = storage_backends
        self._stats = {sbe.name(): {} for sbe in storage_backends}

    @abstractmethod
    def get_description(self):
        """Get description of test."""

    @abstractmethod
    def execute(self):
        """Execute test."""

    def reg_stats(self, sbe, stats_key, stats_val):
        """Register stats (typically elapsed secs for an operation) for a storage backend."""
        self._stats[sbe.name()][stats_key] = stats_val

    def print_stats(self):
        """Print stats collected during text execution."""
        print('TestBase.print_stats() for test {}: ... TODO'.format(self.get_description()))
        pass


class Reset(TestBase):
    def __init__(self, verbose, config, storage_backends, tss):
        super().__init__(verbose, config, storage_backends)
        self._tss = tss

    def get_description(self):
        return 'reset storage backends with {} time series'.format(len(self._tss))

    def execute(self):
        for sbe in self._storage_backends:
            start_secs = common.now_secs()
            sbe.reset(self._tss)
            self.reg_stats(sbe, 'reset secs', common.elapsed_secs(start_secs))


class FillStorage(TestBase):
    def __init__(self, verbose, config, storage_backends, tss, curr_time):
        super().__init__(verbose, config, storage_backends)
        self._tss = tss
        self._curr_time = curr_time

    def get_description(self):
        return 'fill storage with observations'

    def execute(self):
        # fill each time series with observations using the entire accessible capacity
        # ([curr_time - max_age, curr_time])
        ts_data = []
        for ts in self._tss:
            times, obs = ts.create_observations(
                self._curr_time - self._config['max_age'], self._curr_time)
            ts_data.append((ts, times, obs))

        # store the time series in each backend
        for sbe in self._storage_backends:
            start_secs = common.now_secs()
            for td in ts_data:
                sbe.set_observations(td[0], td[1], td[2])
            self.reg_stats(sbe, 'fill storage secs', common.elapsed_secs(start_secs))


class TsTester:
    """Tests/compares different time series storage backends wrt. performance."""

    def __init__(self, verbose, config):
        self._verbose = verbose
        self._config = config
        self._storage_backends = [  # storage backends to test/compare
            PostGIS(
                verbose,
                common.get_env_var('PGHOST', 'localhost'),
                common.get_env_var('PGPORT', '5432'),
                common.get_env_var('PGUSER', 'postgres'),
                common.get_env_var('PGPASSWORD', '', False),
                common.get_env_var('PGDBNAME', 'esoh')
            ),
            NetCDF(verbose),
        ]

    def execute(self):
        """Execute overall test/comparison."""

        tss = create_time_series(self._verbose, self._config)

        test = Reset(self._verbose, self._config, self._storage_backends, tss)
        test.execute()
        test.print_stats()

        curr_time = common.now_secs()

        test = FillStorage(self._verbose, self._config, self._storage_backends, tss, curr_time)
        test.execute()
        test.print_stats()

        # TODO: more tests (subclasses of TestBase):
        # - AppendNewObservations
        # - GetObsInPolygon
        # - GetObsInCircle
        # - GetObsFromAllTimeSeries
        # - GetObsFromStations
        # - GetObsFromParams
        # - GetObsFromStationParams
        # - ...


def create_time_series(verbose, config):
    """Generate a set of time series identified by station/param combos.
    The configuration is used for randomizing ...
        ... the time resolution for each time series, and
        ... the set of params for each station.

    Returns a list of TimeSeries objects.
    """

    nstations = config['nstations']

    time_res = config['time_res']
    time_res = list({int(k): v for k, v in time_res.items()}.items())

    min_params = config['params']['min']
    max_params = config['params']['max']

    param_names = list(map(lambda i: 'param_{}'.format(i), [i for i in range(max_params)]))

    min_lat = config['bbox']['min_lat']
    max_lat = config['bbox']['max_lat']
    if (min_lat < -90) or (min_lat >= max_lat) or (max_lat > 90):
        raise Exception('invalid latitude range in bounding box: [{}, {}]'.format(
            min_lat, max_lat))

    min_lon = config['bbox']['min_lon']
    max_lon = config['bbox']['max_lon']
    if (min_lon < -180) or (min_lon >= max_lon) or (max_lon > 180):
        raise Exception('invalid longitude range in bounding box: [{}, {}]'.format(
            min_lon, max_lon))

    tss = []
    used_locs = set([])  # lat,lon locations used so far

    def create_new_loc():
        """Return a unique lat,lon tuple within the bounding box
        ([min_lat, max_lat] X [min_lon, max_lon]).
        """
        while True:
            lat = min_lat + random.random() * (max_lat - min_lat)
            lon = min_lon + random.random() * (max_lon - min_lon)
            if not (lat, lon) in used_locs:
                break
        used_locs.add((lat, lon))
        return lat, lon

    def create_ts_metadata():
        """Return dict of per time series metadata."""
        return config['ts_metadata']  # ### for now; eventually randomize?

    def create_obs_metadata():
        """Return dict of per observation metadata."""
        return config['obs_metadata']  # ### for now; eventually randomize?

    for s in range(nstations):
        if verbose:
            print('\n\nnext station: {} ...'.format(s))

        lat, lon = create_new_loc()
        random.shuffle(param_names)

        for p in range(random.randint(min_params, max_params)):
            ts_mdata = create_ts_metadata()
            obs_mdata = create_obs_metadata()

            ts = TimeSeries(
                verbose, 'station_{}'.format(s), lat, lon,
                param_names[p], common.select_weighted_value(time_res),
                ts_mdata, obs_mdata
            )
            if verbose:
                print('new ts (s = {}, p = {}): {}'.format(s, p, vars(ts)))

            tss.append(ts)

    return tss
