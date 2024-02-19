from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field


class Type(Enum):
    Feature = "Feature"


class Type1(Enum):
    Point = "Point"


class Geometry(BaseModel):
    type: Type1
    coordinates: Coordinate


class Coordinate(BaseModel):
    lat: float
    lon: float


class Type2(Enum):
    Polygon = "Polygon"


class Geometry1(BaseModel):
    type: Type2
    coordinates: List[Coordinate] = Field(..., min_items=3)


class CreatorType(Enum):
    person = "person"
    group = "group"
    institution = "institution"
    position = "position"


class Properties(BaseModel):
    data_id: str = Field(
        ...,
        description=(
            "Unique identifier of the data as defined by the data producer."
            "Data producers SHOULD NOT use an opaque id, but something meaningful to support client side filtering."
        ),
    )
    title: Optional[str] = Field(
        None,
        description=(
            "short phrase or sentence describing the dataset. In many discovery systems, "
            "the title will be displayed in the results list from a search, "
            "and therefore should be human readable and reasonable to display in a list of such names."
            " This attribute is also recommended by the NetCDF Users Guide and the CF conventions."
        ),
    )
    summary: str = Field(
        ...,
        description="A paragraph describing the dataset, analogous to an abstract for a paper.",
    )
    keywords: Optional[str] = Field(
        None,
        description=(
            "A comma-separated list of key words and/or phrases. Keywords may be common words or phrases, "
            "terms from a controlled vocabulary (GCMD is often used),"
            "or URIs for terms from a controlled vocabulary (see also 'keywords_vocabulary' attribute)."
        ),
    )
    keywords_vocabulary: Optional[str] = Field(
        ...,
        description=(
            "If you are using a controlled vocabulary for the words/phrases in your 'keywords' attribute,"
            " this is the unique name or identifier of the vocabulary from which keywords are taken. "
            "If more than one keyword vocabulary is used, each may be presented with a prefix and a following comma, "
            "so that keywords may optionally be prefixed with the controlled vocabulary key."
        ),
    )
    license: str = Field(
        ...,
        description=(
            "Provide the URL to a standard or specific license, enter 'Freely Distributed' or 'None', "
            "or describe any restrictions to data access and distribution in free text."
        ),
    )
    Conventions: str = Field(
        ...,
        description=(
            "A comma-separated list of the conventions that are followed by the dataset."
            " For files that follow this version of ACDD, "
            "include the string 'ACDD-1.3'. (This attribute is described in the NetCDF Users Guide.)"
        ),
    )
    naming_authority: str = Field(
        ...,
        description=(
            "The organization that provides the initial id (see above) for the dataset."
            " The naming authority should be uniquely specified by this attribute."
            " We recommend using reverse-DNS naming for the naming authority; "
            "URIs are also acceptable. Example: 'edu.ucar.unidata'."
        ),
    )
    creator_type: Optional[CreatorType] = Field(
        None,
        description=(
            "Specifies type of creator with one of the following: 'person', 'group', 'institution', or 'position'. "
            "If this attribute is not specified, the creator is assumed to be a person."
        ),
    )
    creator_name: Optional[str] = Field(
        None,
        description=(
            "The name of the person (or other creator type specified by the creator_type attribute) "
            "principally responsible for creating this data."
        ),
    )
    creator_email: Optional[str] = Field(
        None,
        description=(
            "The email address of the person (or other creator type specified by the creator_type attribute) "
            "principally responsible for creating this data."
        ),
    )
    creator_url: Optional[str] = Field(
        None,
        description=(
            "The URL of the person (or other creator type specified by the creator_type attribute) principally "
            "responsible for creating this data."
        ),
    )
    institution: Optional[str] = Field(
        None,
        description=(
            "The name of the institution principally responsible for originating this data. "
            "This attribute is recommended by the CF convention."
        ),
    )
    project: Optional[str] = Field(
        None,
        description=(
            "The name of the project(s) principally responsible for originating this data."
            " Multiple projects can be separated by commas, as described under Attribute Content Guidelines. "
            "Examples: 'PATMOS-X', 'Extended Continental Shelf Project'."
        ),
    )
    source: Optional[str] = Field(
        None,
        description=(
            "The method of production of the original data. If it was model-generated,"
            " source should name the model and its version. "
            "If it is observational, source should characterize it. This attribute is defined in the CF Conventions."
        ),
    )
    platform: Optional[str] = Field(
        None,
        description=(
            "Name of the platform(s) that supported the sensor data used to create this data set or product. "
            "Platforms can be of any type, including satellite, ship, station, aircraft or other. "
            "Indicate controlled vocabulary used in platform_vocabulary."
        ),
    )
    platform_vocabulary: Optional[str] = Field(
        None,
        description="Controlled vocabulary for the names used in the 'platform' attribute.",
    )
    instrument: Optional[str] = Field(
        None,
        description=(
            "Name of the contributing instrument(s) or sensor(s) used to create this data set or product. "
            "Indicate controlled vocabulary used in instrument_vocabulary."
        ),
    )
    instrument_vocabulary: Optional[str] = Field(
        None,
        description="Controlled vocabulary for the names used in the 'instrument' attribute.",
    )
    history: Optional[str] = Field(
        None,
        description=(
            "Provides an audit trail for modifications to the original data. "
            "This attribute is also in the NetCDF Users Guide: 'This is a character array with a line for each "
            "invocation of a program that has modified the dataset. "
            "Well-behaved generic netCDF applications should append a line containing: date, time of day, user name, "
            "program name and command arguments.' To include a more complete description you can append a reference to"
            " an ISO Lineage entity; see NOAA EDM ISO Lineage guidance."
        ),
    )
    datetime: str = Field(
        ...,
        description="Identifies the date/time of the datas being published, in RFC3339 format.",
    )
    start_datetime: Optional[str] = Field(
        None,
        description="Identifies the start date/time date of the data being published, in RFC3339 format.",
    )
    end_datetime: Optional[str] = Field(
        None,
        description="Identifies the end date/time date of the data being published, in RFC3339 format.",
    )
    processing_level: Optional[str] = Field(
        None,
        description="A textual description of the processing (or quality control) level of the data.",
    )


class Link(BaseModel):
    href: str = Field(..., example="http://data.example.com/buildings/123")
    rel: str = Field(..., example="alternate")
    type: Optional[str] = Field(None, example="application/geo+json")
    hreflang: Optional[str] = Field(None, example="en")
    title: Optional[str] = Field(None, example="Trierer Strasse 70, 53115 Bonn")
    length: Optional[int] = None


class JsonMessageSchema(BaseModel):
    type: Type
    geometry: Union[Geometry, Geometry1]
    properties: Properties
    links: List[Link] = Field(..., min_items=1)
    version: str

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        d = super().dict(*args, **kwargs)
        d["type"] = self.type.value
        d["geometry"]["type"] = self.geometry.type.value
        if isinstance(self.geometry, Geometry):
            d["geometry"]["coordinates"] = self.geometry.coordinates.dict()
        elif isinstance(self.geometry, Geometry1):
            d["geometry"]["coordinates"] = [coord.dict() for coord in self.geometry.coordinates]
        return d
