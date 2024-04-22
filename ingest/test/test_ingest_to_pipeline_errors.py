import pytest
from fastapi import HTTPException


from api.ingest import IngestToPipeline

mqtt_configuration = {
    "host": None,
    "topic": None,
    "username": None,
    "password": None,
}


def test_decide_input_type():
    msg_build = IngestToPipeline(mqtt_configuration, "testing")

    with pytest.raises(HTTPException):
        msg_build._decide_input_type("this_has_no_filetype")
