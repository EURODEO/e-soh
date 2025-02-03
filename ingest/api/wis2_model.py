from typing import List
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from api.model import Link
from api.model import Geometry


class Content(BaseModel):
    encoding: Literal["utf-8", "base64", "gzip"] = Field(..., description="Encoding of content")
    size: int | None = Field(
        None,
        description=(
            "Number of bytes contained in the file. Together with the ``integrity`` property,"
            " it provides additional assurance that file content was accurately received."
            "Note that the limit takes into account the data encoding used, "
            "including data compression (for example `gzip`)."
        ),
    )
    value: str = Field(..., description="The inline content of the file.")
    standard_name: str = Field(..., description="CF standard for the data included in this message.")
    unit: str = Field(..., description="Unit for the data")

    class Config:
        str_strip_whitespace = True


class Properties(BaseModel):
    datetime: str = Field(
        ...,
        description="Identifies the date/time of the data being recorded, in RFC3339 format.",
    )
    pubtime: str = Field(
        ...,
        description="Identifies the date/time of the message being published, in RFC3339 format.",
    )
    data_id: str = Field(
        ...,
        description="Unique identifier of the data as defined by the data producer.",
    )
    metadata_id: Optional[str] = Field(
        ...,
        description="Identifier for associated discovery metadata record to which the notification applies",
    )
    producer: Optional[str] = Field(
        ...,
        description="Identifies the provider that initially captured and processed the source data,"
        " in support of data distribution on behalf of other Members",
    )
    start_datetime: Optional[str] = Field(
        None,
        description="Identifies the start date/time date of the data being recorded, in RFC3339 format.",
    )
    end_datetime: Optional[str] = Field(
        None,
        description="Identifies the end date/time date of the data being recorded, in RFC3339 format.",
    )
    content: Optional[Content] = Field(..., description="Actual data content")
    integrity: Optional[str] = Field(
        None,
        description="Specifies a checksum to be applied to the data to ensure that the download is accurate.",
    )


class Wis2MessageSchema(BaseModel):
    type: Literal["Feature"]
    geometry: Geometry
    properties: Properties
    links: List[Link] = Field(..., min_length=1)
