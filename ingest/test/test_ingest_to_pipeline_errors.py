import pytest
import xarray as xr
from esoh.ingest.main import IngestToPipeline


def test_build_message_errors():
    msg_build = IngestToPipeline(None, None, "testing", testing=True)

    with pytest.raises(TypeError):
        msg_build._build_messages(xr.Dataset())


def test_decide_input_type():
    msg_build = IngestToPipeline(None, None, "testing", testing=True)

    with pytest.raises(ValueError):
        msg_build._decide_input_type("this_has_no_filetype")
