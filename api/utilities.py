import sys

from datetime import datetime
from datetime import timedelta
from typing import Tuple

import datastore_pb2 as dstore
from fastapi import HTTPException
from google.protobuf.timestamp_pb2 import Timestamp
from grpc_getter import get_ts_ag_request
from pydantic import AwareDatetime
from pydantic import TypeAdapter
from pydantic import ValidationError



def get_datetime_range(datetime_string: str | None) -> Tuple[Timestamp, Timestamp] | None:
    if not datetime_string:
        return None

    errors = {}

    start_datetime, end_datetime = Timestamp(), Timestamp()
    aware_datetime_type_adapter = TypeAdapter(AwareDatetime)

    try:
        datetimes = tuple(value.strip() for value in datetime_string.split("/"))
        if len(datetimes) == 1:
            start_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[0]))
            end_datetime.FromDatetime(
                aware_datetime_type_adapter.validate_python(datetimes[0]) + timedelta(seconds=1)
            )  # HACK: Add one second so we get some data, as the store returns [start, end)
        else:
            if datetimes[0] != "..":
                start_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[0]))
            else:
                start_datetime.FromDatetime(datetime.min)
            if datetimes[1] != "..":
                # HACK add one second so that the end_datetime is included in the interval.
                end_datetime.FromDatetime(
                    aware_datetime_type_adapter.validate_python(datetimes[1]) + timedelta(seconds=1)
                )
            else:
                end_datetime.FromDatetime(datetime.max)

            if start_datetime.seconds > end_datetime.seconds:
                errors["datetime"] = f"Invalid range: {datetimes[0]} > {datetimes[1]}"

    except ValueError:
        errors["datetime"] = f"Invalid format: {datetime_string}"

    if errors:
        raise HTTPException(status_code=400, detail=errors)

    return start_datetime, end_datetime


# @cached(ttl=600)
async def get_current_parameter_names():
    """
    This function get a set of standard_names currently in the datastore
    The ttl_hash should be a value that is updated at the same frequency
    we want the lru_cache to be valid for.
    """

    unique_parameter_names = dstore.GetTSAGRequest(attrs=["parameter_name"])
    unique_parameter_names = await get_ts_ag_request(unique_parameter_names)

    return {i.combo.parameter_name for i in unique_parameter_names.groups}


async def verify_parameter_names(parameter_names: list) -> None:
    """
    Function for verifying that the given parameter names are valid.
    Raises error with unknown names if any are found.
    """
    unknown_parameter_names = []
    current_parameter_names = await get_current_parameter_names()

    for i in parameter_names:
        # HACK: This checking breaks the wildcard support (e.g. air_temperature:*:*:*). Skip if contains wildcards
        if "*" in i:
            continue
        if i not in current_parameter_names:
            unknown_parameter_names.append(i)

    if unknown_parameter_names:
        raise HTTPException(400, detail=f"Unknown parameter-name {unknown_parameter_names}")


def create_url_from_request(request):
    # The server root_path contains the path added by a reverse proxy
    base_path = request.scope.get("root_path")

    # The host will (should) be correctly set from X-Forwarded-Host and X-Forwarded-Scheme
    # headers by any proxy in front of it
    host = request.headers["host"]
    scheme = request.url.scheme

    return f"{scheme}://{host}{base_path}/collections"


def split_and_strip(cs_string: str) -> list[str]:
    return [i.strip() for i in cs_string.split(",")]


def validate_bbox(bbox: str) -> Tuple[float, float, float, float]:
    """
    Function for validating the bbox parameter.
    Raises error with invalid bbox if any are found.
    """
    errors = {}

    try:
        left, bottom, right, top = map(float, map(str.strip, bbox.split(",")))
    except ValueError:
        errors["bbox"] = f"Invalid format: {bbox}"
    else:
        if left > right or bottom > top:
            errors["range"] = f"Invalid bbox range: {bbox}"
        if abs(left - right) > 90 or abs(bottom - top) > 90:
            errors["range"] = f"Maximum bbox range is 90 degrees: {bbox}"
        if not -180 <= left <= 180 or not -180 <= right <= 180:
            errors["longitude"] = f"Invalid longitude: {bbox}"
        if not -90 <= bottom <= 90 or not -90 <= top <= 90:
            errors["latitude"] = f"Invalid latitude: {bbox}"

    if errors:
        raise HTTPException(status_code=400, detail=errors)

    return left, bottom, right, top


async def add_parameter_name_and_datetime(request, parameter_name: str | None, datetime: str | None):
    if parameter_name:
        parameter_name = split_and_strip(parameter_name)
        await verify_parameter_names(parameter_name)
        request.filter["parameter_name"].values.extend(parameter_name)

    if datetime:
        start, end = get_datetime_range(datetime)
        request.temporal_interval.start.CopyFrom(start)
        request.temporal_interval.end.CopyFrom(end)


def get_z_range(z: str | None) -> (float, float):
    # it can be z=value1,value2,value3: z=2,10,80 -> not yet implemented for more then one value
    # or z=minimum value/maximum value: z=10/100
    # or z=Rn/min height/height interval: z=R20/100/50  -> not yet implemented
    if z:
        split_on_slash = z.split("/")
        if len(split_on_slash) == 2:
            return float(split_on_slash[0]), float(split_on_slash[1])
        elif len(split_on_slash) > 2:
            # TODO
            raise ValidationError
        split_on_comma = list(map(float, z.split(",")))
        if len(split_on_comma) == 1:
            return float(split_on_comma[0]), float(split_on_comma[0])
        else:
            # TODO
            raise ValidationError
    else:
        return -sys.float_info.max, sys.float_info.max


def is_float(element: any) -> bool:
    if element is None:
        return False
    try:
        float(element)
        return True
    except ValueError:
        return False
