import pytest

from dependencies import parse_parameter_name
from fastapi import HTTPException



@pytest.mark.asyncio
async def test_parse_normal_query():
	expected = ("air_temperature", "2.0", "min", "PT10M")
	parameter_name_input = "air_temperature:2.0:min:PT10M" # minimum air_temperature at 2 meters over 10 minutes
	assert await parse_parameter_name(parameter_name_input) == expected


@pytest.mark.asyncio
async def test_missing_standard_name():
	expected = (None, "2.0", "min", "PT10M")
	parameter_name_input = ":2.0:min:PT10M" # minimum air_temperature at 2 meters over 10 minutes
	assert await parse_parameter_name(parameter_name_input) == expected


@pytest.mark.asyncio
async def test_parse_missing_level():
	expected = ("air_temperature", None, "min", "PT10M")
	parameter_name_input = "air_temperature::min:PT10M" # minimum air_temperature at 2 meters over 10 minutes
	assert await parse_parameter_name(parameter_name_input) == expected


@pytest.mark.asyncio
async def test_parse_missing_function():
	expected = ("air_temperature", "2.0", None, "PT10M")
	parameter_name_input = "air_temperature:2.0::PT10M" # minimum air_temperature at 2 meters over 10 minutes
	assert await parse_parameter_name(parameter_name_input) == expected


@pytest.mark.asyncio
async def test_parse_missing_period():
	expected = ("air_temperature", "2.0", "min", None)
	parameter_name_input = "air_temperature:2.0:min:" # minimum air_temperature at 2 meters over 10 minutes
	assert await parse_parameter_name(parameter_name_input) == expected

@pytest.mark.asyncio
@pytest.mark.parametrize("parameter_name_input, expected", [("atemp:2.0:min:PT10M", HTTPException),
												  ("air_temperature:2.0m:min:PT10M", HTTPException),
												  ("air_temperature:2.0:product:PT10M", HTTPException),
												  ("air_temperature:2.0:min:P10", HTTPException)])
async def test_error_in_parameter_name(parameter_name_input, expected):
	with pytest.raises(expected):
		await parse_parameter_name(parameter_name_input)