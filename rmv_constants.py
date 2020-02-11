from enum import Enum
from dataclasses import dataclass
from collections import namedtuple

BASE_URL = "https://www.rightmove.co.uk/property-to-rent/"
SEARCH_URI = 'find.html'
MAX_RESULTS_PER_PAGE = 24

PROPERTY_ID_FILTER = {"class": "l-searchResult is-list"}
TOTAL_COUNT_FILTER = {"class": "searchHeader-resultCount"}

PROPERTY_DESCRIPTION_FILTER = {"class": "left overflow-hidden agent-content"}
PROPERTY_DETAILS_FILTER = 'RIGHTMOVE.ANALYTICS.DataLayer.pushKV(k,v)'
PROPERTY_IMAGES_FILTER = 'var imageGallery'
PROPERTY_AVAILABILITY_FILTER = "RIGHTMOVE.ANALYTICS.PageViewTracker.trackOnClick('#facebook'"
PROPERTY_FLOORPLAN_FILTER = 'RIGHTMOVE.PROPERTYDETAILS.FloorplanViewer'


Semantic = namedtuple('prop_details', 'rmv_field')


@dataclass
class Coordinates(Enum):
    lat: float = Semantic('"latitude"')
    long: float = Semantic('"longitude"')


# @dataclass
# class PropertyDetails(Enum):
#     def __init__(self, val):
#         geo_lat: float = val
#         geo_long: float = val
#         # geo_location: tuple = Coordinates(Semantic('"latitude"'), Semantic('"longitude"'))
#         postcode: str = val
#         rent_pcm: float = val
#         date_available: int = val
#         rmv_unique_link: int = val
#         estate_agent: str = val
#         estate_agent_address: str = val
#         image_links: [str] = val
#         floorplan_link: str = val


@dataclass
class RmvPropDetails(Enum):
    geo_lat: float = Semantic('"latitude"')
    geo_long: float = Semantic('"longitude"')
    postcode: str = Semantic('"postcode"')
    rent_pcm: float = Semantic('"price"')
    date_available: int = Semantic('"aed"')
    rmv_unique_link: int = Semantic('"propertyId"')
    estate_agent: str = Semantic('"brandName"')
    estate_agent_address = Semantic('"displayAddress"')
    image_links: [str] = Semantic('"masterUrl"')
    floorplan_link: str = Semantic('zoomUrls')
    description: str = Semantic('') #description does not hav
