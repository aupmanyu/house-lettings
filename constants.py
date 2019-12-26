from enum import Enum
from dataclasses import dataclass
from collections import namedtuple

BASE_URL = "https://www.rightmove.co.uk/property-to-rent/"
MAX_RESULTS_PER_PAGE = 24

PROPERTY_ID_FILTER = {"class": "l-searchResult is-list is-not-grid"}
PROPERTY_DETAILS_FILTER = 'RIGHTMOVE.ANALYTICS.DataLayer.pushKV(k,v)'
PROPERTY_IMAGES_FILTER = 'var imageGallery'
PROPERTY_AVAILABILITY_FILTER = "RIGHTMOVE.ANALYTICS.PageViewTracker.trackOnClick('#facebook'"
PROPERTY_FLOORPLAN_FILTER = 'RIGHTMOVE.PROPERTYDETAILS.FloorplanViewer'

Semantic = namedtuple('prop_details', 'rmv_field')


@dataclass
class Coordinates(Enum):
    lat: float = Semantic('"latitude"')
    long: float = Semantic('"longitude"')


@dataclass
class PropertyDetails(Enum):
    geo_lat: float = Semantic('"latitude"')
    geo_long: float = Semantic('"longitude"')
    # geo_location: tuple = Coordinates(Semantic('"latitude"'), Semantic('"longitude"'))
    postcode: str = Semantic('"postcode"')
    rent_pcm: float = Semantic('"price"')
    date_available: int = Semantic('"aed"')
    estate_agent: str = Semantic('"brandName"')
    estate_agent_address = Semantic('"displayAddress"')
    image_links: [str] = Semantic('"masterUrl"')
    floorplan_links: str = Semantic('zoomUrls')


