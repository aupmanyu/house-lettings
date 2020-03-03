from enum import Enum
from dataclasses import dataclass
from collections import namedtuple

Semantic = namedtuple('prop_details', 'zpl_field')


@dataclass
class ZplPropDetails(Enum):
    image_links: [str] = Semantic('Residence, photo')
    estate_agent_address = Semantic('RealEstateAgent, streetAddress;postalCode')
    geo_lat: float = Semantic('Residence, latitude')
    geo_long: float = Semantic('Residence, longitude')
    postcode: str = Semantic('postcode')
    rent_pcm: float = Semantic('price')
    date_available: int = Semantic('aed')
    rmv_unique_link: int = Semantic('propertyId')
    estate_agent: str = Semantic('brandName')
    # image_links: [str] = Semantic('photo')
    floorplan_link: str = Semantic('zoomUrls')