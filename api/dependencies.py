import requests
import json
import isodate

from datetime import datetime
from datetime import timedelta
from typing import Tuple
from functools import lru_cache

from fastapi import HTTPException
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AwareDatetime
from pydantic import TypeAdapter



def get_datetime_range(datetime_string: str | None) -> Tuple[Timestamp, Timestamp] | None:
    if not datetime_string:
        return None

    start_datetime, end_datetime = Timestamp(), Timestamp()
    aware_datetime_type_adapter = TypeAdapter(AwareDatetime)
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
            end_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[1]) + timedelta(seconds=1))
        else:
            end_datetime.FromDatetime(datetime.max)

    return start_datetime, end_datetime

@lru_cache()
def get_nerc_standard_names():
    standard_names_json = json.loads(requests.get("https://vocab.nerc.ac.uk/collection/P07/current/"
                                                  "?_profile=nvs&_mediatype=application%2Fld%2Bjson").content)
    return set([i["prefLabel"]["@value"] for i in standard_names_json["@graph"] if isinstance(i["prefLabel"], dict)])



def parse_parameter_name(parameter_name):
    """
    Function for parsing the aggregate parameter-name field.
    """
    parameter_name = parameter_name.split(":")
    if (n_params := len(parameter_name)) != 4:
        raise HTTPException(400, f"Wrong number of arguemnts in parameter-name, should be 4 got {n_params}")




    standard_name, level, func, period = [i  if not i else None for i in parameter_name]
    func = func.lower()

    errors = []

    if not standard_name in get_nerc_standard_names():
        errors.append(f"Unknown standard_name given, {standard_name}")

    try:
        float(level)
    except ValueError:
        errors.append(f"Level could not be converted to float")

    if func not in (legal_func := ["max", "min", "average", "instantaneous", "mean"]):
        errors.append(f"Unknown function given, {func}, has to be one of {legal_func}")

    try:
        isodate.parse_duration(period)
    except isodate.ISO8601Error:
        errors.append(f"Invalid ISO8601 duration")

    if not errors:
        raise HTTPException(400, detail="\n".join(errors))

    return standard_name, level, func, period
