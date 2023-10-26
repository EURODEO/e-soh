from edr_pydantic.capabilities import Contact
from edr_pydantic.capabilities import LandingPageModel
from edr_pydantic.capabilities import Provider
from edr_pydantic.link import Link


def get_landing_page(request):
    return LandingPageModel(
        title="E-SOH EDR API",
        description="The E-SOH EDR API",
        keywords=["weather", "temperature", "wind", "humidity", "pressure", "clouds", "radiation"],
        provider=Provider(name="RODEO", url="https://rodeo-project.eu/"),
        contact=Contact(email="rodeoproject@fmi.fi"),
        links=[
            Link(href=f"{request.url}", rel="self", title="Landing Page in JSON"),
            Link(href=f"{request.url}docs", rel="service-desc", title="API description in HTML"),
            Link(href=f"{request.url}openapi.json", rel="service-desc", title="API description in JSON"),
            # Link(href=f"{request.url}conformance", rel="data", title="Conformance Declaration in JSON"),
            Link(href=f"{request.url}collections", rel="data", title="Collections metadata in JSON"),
        ],
    )
