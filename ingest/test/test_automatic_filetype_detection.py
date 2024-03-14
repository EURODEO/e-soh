import pytest

from api.ingest import IngestToPipeline


@pytest.mark.parametrize(
    "test_inpt, expected",
    [
        ("test/test_data/knmi/20230101.nc", "netCDF"),
        ("test/test_data/bufr/SurfaceSee_subset_12.buf", "bufr"),
        ("test/test_data/bufr/SYNOP_BUFR_2718.bufr", "bufr"),
        ("test/test_data/bufr/data0100", "bufr"),
        ("test/test_data/knmi/20230102.nc", "netCDF"),
        ("test/test_data/knmi/20221231.nc", "netCDF"),
        ("test/test_data/met_norway/" + "air_temperature_gullingen_skisenter-parent.nc", "netCDF"),
    ],
)
def test_decide_input_type(test_inpt, expected):
    msg_build = IngestToPipeline(None, None, "testing", testing=True)
    assert msg_build._decide_input_type(test_inpt) == expected
