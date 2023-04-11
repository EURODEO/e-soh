import common
import random
import time
from postgis import PostGIS
from netcdf import NetCDF
from timeseries import TimeSeries


class TsTester:
    """Tests/compares different time series storage backends wrt. performance."""

    def __init__(self, verbose, cfg):
        self._verbose = verbose
        self._cfg = cfg  # configuration
        self._sbes = [PostGIS(verbose), NetCDF(verbose)]  # storage backends to test/compare

    def execute(self):
        """Execute overall test/comparison."""
        self.__populate()
        self.__retrieve()

    def __populate(self):
        """Populate storage backends with data to test.

        Generate a set of time series and have all storage backends register them
        and fill them with observations. The number of time series and observations
        are regulated/randomized according to the configuration (self._cfg).
        """
        nstations = self._cfg['nstations']
        time_res = self._cfg['time_res']
        time_res = list({int(k): v for k, v in time_res.items()}.items())

        min_params = self._cfg['params']['min']
        max_params = self._cfg['params']['max']
        param_names = list(map(lambda i: 'param_{}'.format(i), [i for i in range(max_params)]))

        min_lat = self._cfg['bbox']['min_lat']
        max_lat = self._cfg['bbox']['max_lat']
        min_lon = self._cfg['bbox']['min_lon']
        max_lon = self._cfg['bbox']['max_lon']
        if (min_lat < -90) or (min_lat >= max_lat) or (max_lat > 90):
            raise Exception('invalid latitude range in bounding box: [{}, {}]'.format(
                min_lat, max_lat))
        if (min_lon < -180) or (min_lon >= max_lon) or (max_lon > 180):
            raise Exception('invalid longitude range in bounding box: [{}, {}]'.format(
                min_lon, max_lon))

        def create_time_series():
            """Generate a set of time series identified by station/param combos.
            The configuration is used for randomizing:
                - the time resolution for each time series, and
                - the set of params for each station.

            Returns a list of TimeSeries objects.
            """

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
                return self._cfg['ts_metadata']  # ### for now; eventually randomize?

            def create_obs_metadata():
                """Return dict of per observation metadata."""
                return self._cfg['obs_metadata']  # ### for now; eventually randomize?

            for s in range(nstations):

                if self._verbose:
                    print('\n\nnext station: {} ...'.format(s))

                lat, lon = create_new_loc()

                random.shuffle(param_names)
                for p in range(random.randint(min_params, max_params)):

                    ts_mdata = create_ts_metadata()
                    obs_mdata = create_obs_metadata()

                    ts = TimeSeries(
                        self._verbose, 'station_{}'.format(s), lat, lon,
                        param_names[p], common.select_weighted_value(time_res),
                        ts_mdata, obs_mdata
                    )
                    if self._verbose:
                        print('new ts (s = {}, p = {}): {}'.format(s, p, vars(ts)))
                    tss.append(ts)

            return tss

        # create a set of time series and register them in each storage backend
        tss = create_time_series()
        for sbe in self._sbes:
            sbe.reset(tss)

        # create observations for each time series and register them in each storage backend
        # (note: observations are created according to the properties of each specific time series,
        # such as time resolution)
        now = int(time.time())  # current UNIX timestamp in secs
        for ts in tss:
            obs = ts.create_observations(now - self._cfg['ssize'], now)  # fill entire storage
            for sbe in self._sbes:
                sbe.set_observations(ts, obs)

    def __retrieve(self):
        """Test/compare storage backends wrt. different use cases for data retrieval."""
        pass
