import json
import logging

from . import viewings
from util import loggers


loggers.init_root_logger()

logger = logging.getLogger("nectr")

with open("calendly_property_mapping.json") as f:
    calendly_property_mapping = json.loads(f)


def booking_handler(booking_info: dict):
    try:
        property_id = calendly_property_mapping[booking_info["payload"]["event_type"]["uuid"]]
        viewings.create_booking(property_id, booking_info)

    except KeyError as e:
        logger.critical("Could not find this property in CMS", e)