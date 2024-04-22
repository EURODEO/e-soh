import pytest
from fastapi import HTTPException

from api.main import _decide_input_type

mqtt_configuration = {
    "host": None,
    "topic": None,
    "username": None,
    "password": None,
}


@pytest.mark.parametrize(
    "test_inpt, expected",
    [
        ("test/test_data/bufr/SurfaceSee_subset_12.buf", "bufr"),
        ("test/test_data/bufr/SYNOP_BUFR_2718.bufr", "bufr"),
        ("test/test_data/bufr/data0100", "bufr"),
    ],
)
def test_decide_input_type(test_inpt, expected):
    assert _decide_input_type(test_inpt) == expected


@pytest.mark.parametrize(
    "test_inpt",
    ["test/test_data/knmi/20230101.nc", "test/test_data/knmi/20230102.nc", "this_has_no_filetype", "netCDF"],
)
def test_decide_input_type_raise_exception(test_inpt):
    with pytest.raises(HTTPException):
        _decide_input_type(test_inpt)
