import re
import datetime

import rmv_constants
import general_constants


def keyword_filter(keyword: general_constants.Keywords, description: str) -> bool:
    # description = property_listing[rmv_constants.RmvPropDetails.description.name]
    if keyword is general_constants.Keywords.GARDEN:
        neg_lookbehind_list = ['no', 'hatton']
        neg_lookbehind_expr = ('(?<!{})' * len(neg_lookbehind_list)).format(*neg_lookbehind_list)
        match = re.search(r'{}(\sgardens*\b)'.format(neg_lookbehind_expr), description, re.IGNORECASE)
        return bool(match)

    elif keyword is general_constants.Keywords.PARKING_SPACE:
        match = re.search(r'\w*(?<!no)(\sparking)', description, re.IGNORECASE)
        return bool(match)

    elif keyword is general_constants.Keywords.CONCIERGE:
        return "concierge" in description

    elif keyword is general_constants.Keywords.NO_GROUND_FLOOR:
        return "ground floor" not in description

    else:
        return False


def date_available_filter(property_listing, lower_threshold, upper_threshold):
    available_date = datetime.datetime.strptime(property_listing[rmv_constants.RmvPropDetails.date_available.name],
                                                "%Y-%m-%d %H:%M:%S")
    lower_date_threshold = datetime.datetime.strptime(lower_threshold, "%Y-%m-%d %H:%M:%S")
    upper_date_threshold = datetime.datetime.strptime(upper_threshold, "%Y-%m-%d %H:%M:%S")

    return lower_date_threshold <= available_date <= upper_date_threshold


def enough_images_filter(property_listing, threshold):
    try:
        return len(property_listing[rmv_constants.RmvPropDetails.image_links.name]) > threshold

    except TypeError as e:
        print("Images filter: An error occurred filtering property: {}. CULPRIT: {} ".format(e, property_listing))
        return False


def floorplan_filter(property_listing):
    try:
        return len(property_listing[rmv_constants.RmvPropDetails.floorplan_link.name]) > 0

    except TypeError as e:
        print("Floorplan filter: An error occurred filtering property: {}. CULPRIT: {} ".format(e, property_listing))
        return False


def min_rent_filter(property_listing, threshold):
    try:
        return float(property_listing[rmv_constants.RmvPropDetails.rent_pcm.name]) > threshold

    # TODO: ValueError caught and returned as False for properties where rent is 'null' for unknown reasons.
    #  For now, we ignore these properties during filtering. Once we debug the 'null' rent issue,
    #  ValueError no longer needs to be caught
    except (TypeError, ValueError) as e:
        print("Min rent filter: An error occurred filtering property: {}. CULPRIT: {} ".format(e, property_listing))
        return False
