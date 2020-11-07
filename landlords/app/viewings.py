import json
import shortuuid
import logging

from . import constants
from webflow import cms


def create_booking(property_id: str, booking_details: dict):
    booking_id = shortuuid.uuid()
    payload = {
        "name": booking_id,
        "slug": booking_id,
        "property-id": property_id,
        "assigned-nectarine": booking_details.get("nectarine"),
        "booking-datetime": booking_details.get("start_time_pretty"),
        "viewer-name": booking_details.get("invitee").get("name"),
        "viewer-email": booking_details.get("invitee").get("email")
    }

    item_id = cms.create_item(payload, constants.WEBFLOW_BOOKINGS_COLLECTION)



