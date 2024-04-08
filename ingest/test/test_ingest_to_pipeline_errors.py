import pytest
import xarray as xr
from fastapi import HTTPException


from api.ingest import IngestToPipeline


def test_build_message_errors():
    msg_build = IngestToPipeline(None, "testing", testing=True)

    with pytest.raises(HTTPException):
        msg_build._build_messages(xr.Dataset())


def test_decide_input_type():
    msg_build = IngestToPipeline(None, "testing", testing=True)

    with pytest.raises(HTTPException):
        msg_build._decide_input_type("this_has_no_filetype")
