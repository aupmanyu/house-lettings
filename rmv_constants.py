from uuid import UUID
from enum import Enum, auto
from dataclasses import dataclass
from collections import namedtuple

BASE_URL = "https://www.rightmove.co.uk/property-to-rent"
FIND_URI = '/find.html'
SEARCH_URL = 'https://where.rightmove.co.uk/search'
MAX_RESULTS_PER_PAGE = 24

PROPERTY_ID_FILTER = {"class": "l-searchResult is-list"}
TOTAL_COUNT_FILTER = {"class": "searchHeader-resultCount"}

PROPERTY_DESCRIPTION_FILTER = {"class": "left overflow-hidden agent-content"}
PROPERTY_DETAILS_FILTER = 'RIGHTMOVE.ANALYTICS.DataLayer.pushKV(k,v)'
PROPERTY_IMAGES_FILTER = 'var imageGallery'
PROPERTY_AVAILABILITY_FILTER = "RIGHTMOVE.ANALYTICS.PageViewTracker.trackOnClick('#facebook'"
PROPERTY_FLOORPLAN_FILTER = 'RIGHTMOVE.PROPERTYDETAILS.FloorplanViewer'


GMAPS_DISTANCE_MATRIX_MAX_ORIGINS = 25


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

class AutoEnum(Enum):
    def _generate_next_value_(name, start, count, last_values):
        # Always unique identifier that refers to a Python object and hence unlikely to turn up anywhere else on RMV
        return object()


# NB: auto() needed so that Enum can distinguish them as individual items (using empty string, for e.g., doesn't work
# as Enum considers items as aliases of each other.
# TODO: The auto() is inside the Semantic() encapsulation since rest of code uses this to parse details back from RMV.
#  Should remove this.
class RmvPropDetails(AutoEnum):
    zone_best_guess: int = Semantic(auto())  # zone is not available in JS scripts and derived from another source
    prop_uuid: UUID = Semantic(auto())
    geo_lat: float = Semantic('"latitude"')
    geo_long: float = Semantic('"longitude"')
    postcode: str = Semantic('"postcode"')
    rent_pcm: float = Semantic('"price"')
    beds: int = Semantic('"beds"')
    date_available: str = Semantic('"aed"')
    rmv_unique_link: str = Semantic('"propertyId"')
    url: str = Semantic(auto())  # url is not available in JS scripts so generated elsewhere
    estate_agent: str = Semantic('"brandName"')
    estate_agent_address: str = Semantic('"displayAddress"')
    image_links: [str] = Semantic('"masterUrl"')
    floorplan_links: [str] = Semantic('zoomUrls')
    description: str = Semantic(auto())  # description does not have an identifier in JS scripts


class RmvTransportModes(Enum):
    transit = 'public_transport,driving_train'
    walking = 'walking'
    bicycling = 'cycling'
    driving = 'driving'

