import pytest
from dependencies import parse_parameter_name
from fastapi import HTTPException


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parameter_name_input, expected",
    [
        ("atemp:2.0:min:PT10M", HTTPException),
        ("air_temperature:2.0m:min:PT10M", HTTPException),
        ("air_temperature:2.0:product:PT10M", HTTPException),
        ("air_temperature:2.0:min:P10", HTTPException),
        (":", HTTPException),  # Too few parameters
        ("::::::", HTTPException),
    ],
)  # Too many parameter
async def test_error_in_parameter_name(parameter_name_input, expected):
    with pytest.raises(expected):
        await parse_parameter_name(parameter_name_input)
