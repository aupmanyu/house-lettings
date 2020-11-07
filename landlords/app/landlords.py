import shortuuid

from . import constants
from webflow import cms


def get_landlords():
    landlords = cms.get_items(constants.WEBFLOW_LANDLORDS_COLLECTION)
    return landlords


def add_landlord(landlord_details: dict):
    landlord_uuid = shortuuid.uuid()
    payload = {
        "name": landlord_uuid,
        "slug": landlord_uuid,
        "landlord-name": landlord_details["name"],
        "landlord-mobile": landlord_details["mobile"]
    }

    item_id = cms.create_item(payload, constants.WEBFLOW_LANDLORDS_COLLECTION)
    return item_id