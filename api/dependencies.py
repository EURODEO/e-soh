from typing import Tuple
from datetime import datetime, timedelta
from pydantic import AwareDatetime
from pydantic import TypeAdapter
from google.protobuf.timestamp_pb2 import Timestamp


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
            end_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(
                datetimes[1]) + timedelta(seconds=1))
        else:
            end_datetime.FromDatetime(datetime.max)

    return start_datetime, end_datetime
