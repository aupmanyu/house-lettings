from multiprocessing import Pool, TimeoutError
import requests
import dataclasses
import json
from calmjs.parse import es5
from calmjs.parse.asttypes import Object, VarDecl, FunctionCall, Arguments, Assign, GetPropAssign, PropIdentifier
from calmjs.parse.walkers import Walker
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from bs4 import BeautifulSoup

import constants


# "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=POSTCODE%5E1218949&radius=0.5""


def get_properties_summary(url: str, postcode_identifier: str, **kwargs):
    """
    Gets the summary page from Rightmove and filters by xpath_property_card HTML div
    to get to the Rightmove-specific unique IDs for each property
    """

    xpath_property_card = constants.PROPERTY_ID_FILTER
    properties_id_list = []
    payload = {
        "locationIdentifier": postcode_identifier.replace(' ', ''),
        "radius": kwargs['radius'] if 'radius' in kwargs else 0,
        "index": kwargs['index'] if 'index' in kwargs else 0
    }
    data = requests.get(url, payload)
    soup = BeautifulSoup(data.text, "html.parser")
    properties_soup = soup.find_all("div", xpath_property_card)
    for prop in properties_soup:
        properties_id_list.append(prop.get('id'))
    return properties_id_list


def get_property_details(property_id: str):
    url = constants.BASE_URL + property_id + '.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/60.0.3112.113 Safari/537.36'
    }
    data = requests.get(url, headers=headers)
    soup = BeautifulSoup(data.text, "html.parser")
    scripts_soup = soup.find_all('script')
    scripts_with_details = list(filter(lambda x: True if x.find(constants.PROPERTY_DETAILS_FILTER) >= 0 else False,
                                       [str(scripts_soup[y].next).strip().replace('\r', '')
                                       .replace('\n', '')
                                       .replace('\t', '')
                                        for y in range(0, len(scripts_soup))]))

    # the field we want is repeated many times in this script so we just pick one
    # (use 6th element because it's the first occurence of clean JS code that doesn't cause the parser to break)
    scripts_with_availability = \
    list(filter(lambda x: True if x.find(constants.PROPERTY_AVAILABILITY_FILTER) >= 0 else False,
                [str(scripts_soup[y].next).strip().replace('\r', '')
                .replace('\n', '')
                .replace('\t', '')
                 for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')[6]

    # hacks because there is a JS error in this script (missing semicolon) further down (around char 10710)
    # that causes parser to break
    try:
        scripts_with_images = list(filter(lambda x: True if x.find(constants.PROPERTY_IMAGES_FILTER) >= 0 else False,
                                          [str(scripts_soup[y].next).strip().replace('\r', '')
                                          .replace('\n', '')
                                          .replace('\t', '')
                                           for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')
        scripts_with_images = list(
            filter(lambda x: True if x.find(constants.PROPERTY_IMAGES_FILTER) >= 0 else False,
                   [y for y in scripts_with_images]))

    except IndexError:
        scripts_with_images = []

    try:
        scripts_with_floorplans = list(filter(lambda x: True if x.find(constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                                              [str(scripts_soup[y].next).strip().replace('\r', '')
                                              .replace('\n', '')
                                              .replace('\t', '')
                                               for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')
        scripts_with_floorplans = list(
            filter(lambda x: True if x.find(constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                   [y for y in scripts_with_floorplans]))
    except IndexError:
        scripts_with_floorplans = []

    scripts_to_walk = scripts_with_details + [scripts_with_availability] + \
                      scripts_with_images + scripts_with_floorplans

    walker = Walker()
    tree = [es5(script) for script in scripts_to_walk]
    property_listing = {}
    for tree_node in tree:
        for node in walker.filter(tree_node, lambda x: isinstance(x, Assign)):
            for field in constants.PropertyDetails:
                if field.value.rmv_field == node.left.value:
                    if field.name == constants.PropertyDetails.image_links.name:
                        if field.name in property_listing:
                            (property_listing[field.name]).append(node.right.value.replace('"', ''))
                        else:
                            property_listing[field.name] = [node.right.value.replace('"', '')]
                    elif field.name == constants.PropertyDetails.floorplan_links.name:
                        property_listing[field.name] = [link.value.replace('"', '') for link in node.right.items]
                    else:
                        property_listing[field.name] = str(node.right.value).replace('"', '')
                    break

    print(property_listing)


# def construct_property_listing(data_model):
#     property_listing = {}
#     for field in data_model:
#         property_listing[field]
# for node in walker.filter(es5(split_script[1]), lambda x: (isinstance(x, FunctionCall))):
#     image_links = []
#     for subnode in walker.filter(node, lambda x: isinstance(x, Assign)):
#         if subnode.left.value == 'images':
#             for link_node in walker.filter(subnode, lambda x: isinstance(x, Assign)):
#                 if link_node.left.value == '"masterUrl"':
#                     image_links.append(link_node.right.value)
#             # intermediate_image_nodes = subnode.right.items
#             # image_links = [link.right.value for link in [walker.filter(child_node, lambda x: isinstance(x, Assign))
#             #                for child_node in intermediate_image_nodes] if link.left.value == 'masterUrl']
#             print(image_links)

# intermediate_image_links = [arg.right.items for arg in walker.filter(node, lambda x: (isinstance(x, Assign)))
#            if arg.left.value == 'images']
# image_links = [walker.extract(link, lambda x: isinstance(x, Assign)) for link in intermediate_image_links]
# # image_links = [link for link in walker.extract(intermediate_image_links[])]

# my_args = json.loads(node.args)

# return details


# def get_property_images():
#     images = soup.find_all('a')

# property_id_list = get_property_list(BASE_URL + 'find.html', 'POSTCODE^1218949', radius=0.5)
# [get_property_details(property_id) for property_id in property_id_list]
# results = get_soup_webpage(url_summary.format(0))
# print(results)
get_property_details('property-76133335')
get_property_details('property-45850637')
get_property_details('property-43602508')
get_property_details('property-47191512')
test = dataclasses.fields(constants.PropertyDetails)

# if __name__ == "__main__":
#    pass
