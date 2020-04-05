import os
from enum import Enum

DB_URL = os.environ['DATABASE_URL']
NEIGHBOURHOODS_URL = "https://en.wikipedia.org/wiki/List_of_areas_of_London"


class PropertyStatus(Enum):
    liked = 'liked'
    disliked = 'disliked'
    superliked = 'superliked'


class CheckboxFeatures(Enum):
    WOODEN_FLOORS = 1
    NO_GROUND_FLOOR = 2
    OPEN_PLAN_KITCHEN = 3
    GARDEN = 4
    PROXIMITY_GYM = 5
    NO_LOUD_STREET = 6
    PROXIMITY_PARK = 7
    BRIGHT = 8
    MODERN_INTERIORS = 9
    PROXIMITY_SUPERMARKET = 10
    PARKING_SPACE = 11
    CONCIERGE = 12


