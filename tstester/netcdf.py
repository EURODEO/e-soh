import netCDF4 as nc


class NetCDF:
    """Access to netCDF files"""

    def __init__(self, verbose):
        self._verbose = verbose

    def create_initial_file(self, path, ts):
        """Create the initial file for a time series."""

        with nc.Dataset(path, 'w') as dset:

            dset.setncatts({
                'station_id': ts.station_id(),
                'param_id': ts.param_id(),
                'spatial_representation': 'point',
                'geospatial_lat_min': ts.lat(),
                'geospatial_lat_max': ts.lat(),
                'geospatial_lon_min': ts.lon(),
                'geospatial_lon_max': ts.lon(),
            })

            vlat = dset.createVariable('latitude', 'f')
            vlat.standard_name = 'latitude'
            vlat.long_name = 'station latitude'
            vlat.units = 'degrees_north'
            vlat[:] = ts.lat()

            vlon = dset.createVariable('longitude', 'f')
            vlon.standard_name = 'longitude'
            vlon.long_name = 'station longitude'
            vlon.units = 'degrees_east'
            vlon[:] = ts.lon()
