import datetime

import rmv_constants

def keyword_filter(keywords: list, description: str):
    if all(x in description for x in keywords):
        return True


def date_available_filter(property_listing, lower_threshold, upper_threshold):
    available_date = datetime.datetime.strptime(property_listing[rmv_constants.RmvPropDetails.date_available.name],
                                                "%Y-%m-%d-%H-%M-%S")
    lower_date_threshold = datetime.datetime.strptime(lower_threshold, "%Y-%m-%d-%H-%M-%S")
    upper_date_threshold = datetime.datetime.strptime(upper_threshold, "%Y-%m-%d-%H-%M-%S")

    if not (lower_date_threshold <= available_date <= upper_date_threshold):
        return False
    else:
        return True


def enough_images_filter(property_listing, threshold):
    if rmv_constants.RmvPropDetails.image_links.name not in property_listing or \
            len(property_listing[rmv_constants.RmvPropDetails.image_links.name]) < threshold:
        return False
    else:
        return True


def floorplan_filter(property_listing):
    if rmv_constants.RmvPropDetails.floorplan_link.name not in property_listing or\
            len(property_listing[rmv_constants.RmvPropDetails.floorplan_link.name]) < 1:
        return False
    else:
        return True


def min_rent_filter(property_listing, threshold):
    if rmv_constants.RmvPropDetails.rent_pcm.name not in property_listing or \
            property_listing[rmv_constants.RmvPropDetails.rent_pcm.name] < threshold:
        return False
    else:
        return True
