import pytest

from esoh.ingest.main import ingest_to_pipeline


@pytest.mark.parametrize("test_inpt, expected", [('test/test_data/knmi/20230101.nc', 'netCDF'),
                                                 ('test/test_data/knmi/20230102.nc', 'netCDF'),
                                                 ('test/test_data/knmi/20221231.nc', 'netCDF'),
                                                 ('test/test_data/met_norway/'
                                                 + 'air_temperature_gullingen_skisenter-parent.nc',
                                                  'netCDF')])
def test_decide_input_type(test_inpt, expected):
    msg_build = ingest_to_pipeline(None, None, "testing", testing=True)
    assert msg_build._decide_input_type(test_inpt) == expected
