import asyncio
import requests
import json
import isodate

import protobuf.datastore_pb2 as dstore

from datetime import datetime
from datetime import timedelta
from typing import Tuple
from functools import lru_cache

from fastapi import HTTPException
from google.protobuf.timestamp_pb2 import Timestamp
from grpc_getter import getTSAGRequest
from pydantic import AwareDatetime
from pydantic import TypeAdapter


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
                start_datetime.FromDatetime(
                    aware_datetime_type_adapter.validate_python(datetimes[0])
                )
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


async def get_current_parameter_names(ttl_hash=None):
    """
    This function get a set of standard_names currently in the datastore
    The ttl_hash should be a value that is updated at the same frequency
    we want the lru_cache to be valid for.
    """

    @lru_cache(maxsize=1)
    async def async_helper(ttl_hash):
        del ttl_hash  # make linter think we used this value
        unique_parameter_names = dstore.GetTSAGRequest(attrs=["parameter_name"])
        unique_parameter_names = await getTSAGRequest(unique_parameter_names)

        return set([i.combo.standard_name for i in unique_parameter_names.groups])

    return await async_helper(ttl_hash)


async def verify_parameter_names(parameter_names: list) -> None:
    """
    Function for verifying that the given parameter names are valid.
    Raises error with unknown names if any are found.
    """
    unknown_parameter_names = []

    for i in parameter_names:
        if i not in await get_current_parameter_names(datetime.now().hour):
            unknown_parameter_names.append(i)

    if unknown_parameter_names:
        raise HTTPException(400, detail=f"Unknown parameter-name {unknown_parameter_names}")
