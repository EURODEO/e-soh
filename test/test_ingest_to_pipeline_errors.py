import pytest

from esoh.ingest.main import ingest_to_pipeline
import xarray as xr


def test_build_message_errors():
    msg_build = ingest_to_pipeline(None, None, "testing", testing=True)

    with pytest.raises(TypeError):
        msg_build._build_messages(xr.Dataset())


def test_decide_input_type():
    msg_build = ingest_to_pipeline(None, None, "testing", testing=True)

    with pytest.raises(ValueError):
        msg_build._decide_input_type("this_has_no_filetype")
