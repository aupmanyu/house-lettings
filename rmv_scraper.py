from multiprocessing import Pool, TimeoutError
import csv
import math
import random
import requests
import datetime
import dataclasses
import json
from calmjs.parse import es5
from calmjs.parse.asttypes import Object, VarDecl, FunctionCall, Arguments, Assign, GetPropAssign, PropIdentifier
from calmjs.parse.walkers import Walker
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from bs4 import BeautifulSoup

import rmv_constants


# "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=POSTCODE%5E1218949&radius=0.5""


def get_properties_summary(url: str, postcode_identifier: str, **kwargs):
    """
    Gets the summary page from Rightmove and filters by xpath_property_card HTML div
    to get to the Rightmove-specific unique IDs for each property
    """
    headers = {
        'User-Agent': gen_random_user_agent()
    }
    xpath_property_card = rmv_constants.PROPERTY_ID_FILTER
    xpath_total_count = rmv_constants.TOTAL_COUNT_FILTER
    properties_id_list = []
    payload = {
        "locationIdentifier": postcode_identifier.replace(' ', ''),
        "radius": kwargs['radius'] if 'radius' in kwargs else 0,
        "index": kwargs['index'] if 'index' in kwargs else 0,
        "minBedrooms": kwargs['minBedrooms'] if 'minBedrooms' in kwargs else None,
        "maxPrice": kwargs['maxPrice'] if 'maxPrice' in kwargs else None,
        "keywords": kwargs['keywords'] if 'keywords' in kwargs else None
    }
    data = requests.get(url, headers=headers, params=payload)
    soup = BeautifulSoup(data.text, "html.parser")
    properties_soup = soup.find_all("div", xpath_property_card)
    total_count = int((soup.find("span", xpath_total_count)).contents[0])
    for prop in properties_soup:
        properties_id_list.append(prop.get('id'))

    iterations = math.ceil(total_count / len(properties_id_list))

    # print("Total results returned are {} so making {} more iterations".format(total_count, iterations - 1))

    while iterations > 1:
        # print("Iteration {}".format(iterations))
        headers = {
            'User-Agent': gen_random_user_agent()
        }
        payload = {
            "locationIdentifier": postcode_identifier.replace(' ', ''),
            "radius": kwargs['radius'] if 'radius' in kwargs else 0,
            "index": iterations,
            "minBedrooms": kwargs['minBedrooms'] if 'minBedrooms' in kwargs else None,
            "maxPrice": kwargs['maxPrice'] if 'maxPrice' in kwargs else None,
            "keywords": kwargs['keywords'] if 'keywords' in kwargs else None
        }

        data = requests.get(url, headers=headers, params=payload)
        # print("Got some data back")
        soup = BeautifulSoup(data.text, "html.parser")
        properties_soup = soup.find_all("div", xpath_property_card)

        for prop in properties_soup:
            properties_id_list.append(prop.get('id'))

        iterations -= 1

    return properties_id_list


def get_property_details(property_id: str, keywords: list):
    # url = rmv_constants.BASE_URL + property_id + '.html'
    url = property_id
    headers = {
        'User-Agent': gen_random_user_agent()
    }
    xpath_description = rmv_constants.PROPERTY_DESCRIPTION_FILTER
    data = requests.get(url, headers=headers)
    soup = BeautifulSoup(data.text, "html.parser")
    description_text = soup.find("div", xpath_description).text

    scripts_soup = soup.find_all('script')
    scripts_with_details = list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_DETAILS_FILTER) >= 0 else False,
                                       [str(scripts_soup[y].next).strip().replace('\r', '')
                                       .replace('\n', '')
                                       .replace('\t', '')
                                        for y in range(0, len(scripts_soup))]))

    # the field we want is repeated many times in this script so we just pick one
    # (use 6th element because it's the first occurence of clean JS code that doesn't cause the parser to break)
    scripts_with_availability = \
    list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_AVAILABILITY_FILTER) >= 0 else False,
                [str(scripts_soup[y].next).strip().replace('\r', '')
                .replace('\n', '')
                .replace('\t', '')
                 for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')[6]

    # hacks because there is a JS error in this script (missing semicolon) further down (around char 10710)
    # that causes parser to break
    try:
        scripts_with_images = list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_IMAGES_FILTER) >= 0 else False,
                                          [str(scripts_soup[y].next).strip().replace('\r', '')
                                          .replace('\n', '')
                                          .replace('\t', '')
                                           for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')
        scripts_with_images = list(
            filter(lambda x: True if x.find(rmv_constants.PROPERTY_IMAGES_FILTER) >= 0 else False,
                   [y for y in scripts_with_images]))

    except IndexError:
        scripts_with_images = []

    try:
        scripts_with_floorplans = list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                                              [str(scripts_soup[y].next).strip().replace('\r', '')
                                              .replace('\n', '')
                                              .replace('\t', '')
                                               for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')
        scripts_with_floorplans = list(
            filter(lambda x: True if x.find(rmv_constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                   [y for y in scripts_with_floorplans]))
    except IndexError:
        scripts_with_floorplans = []

    scripts_to_walk = scripts_with_details + [scripts_with_availability] + \
                      scripts_with_images + scripts_with_floorplans

    walker = Walker()
    tree = [es5(script) for script in scripts_to_walk]
    property_listing = {}
    made_the_cut = []
    ignored = []
    for tree_node in tree:
        for node in walker.filter(tree_node, lambda x: isinstance(x, Assign)):
            for field in rmv_constants.RmvPropDetails:
                if field.value.rmv_field == node.left.value:
                    if field.name == rmv_constants.RmvPropDetails.image_links.name:
                        if field.name in property_listing:
                            (property_listing[field.name]).append(node.right.value.replace('"', ''))
                        else:
                            property_listing[field.name] = [node.right.value.replace('"', '')]
                    elif field.name == rmv_constants.RmvPropDetails.floorplan_link.name:
                        property_listing[field.name] = [link.value.replace('"', '') for link in node.right.items]
                    else:
                        property_listing[field.name] = str(node.right.value).replace('"', '')
                    break
    if keyword_filter(keywords, description_text):
        if last_stage_filter(property_listing):
            made_the_cut.append(url)
            print(made_the_cut[0])
        else:
            ignored.append(url)
    else:
        ignored.append(url)
    # print("Ignored ", ignored)
        # return property_listing[rmv_constants.RmvPropDetails.rmv_unique_link.name]
    # print(property_listing)


def keyword_filter(keywords: list, description: str):
    if all(x in description for x in keywords):
        return True

def last_stage_filter(property_listing):
    available_date = datetime.datetime.strptime(property_listing[rmv_constants.RmvPropDetails.date_available.name],
                                                "%Y-%m-%d-%H-%M-%S")
    lower_date_threshold = datetime.datetime.strptime('2020-02-01-00-00-00', "%Y-%m-%d-%H-%M-%S")
    upper_date_threshold = datetime.datetime.strptime('2020-03-18-00-00-00', "%Y-%m-%d-%H-%M-%S")
    try:
        if not rmv_constants.RmvPropDetails.image_links.name in property_listing or \
                len(property_listing[rmv_constants.RmvPropDetails.image_links.name]) < 5:
            return False
        # elif not rmv_constants.RmvPropDetails.floorplan_link.name in property_listing or\
        #         len(property_listing[rmv_constants.RmvPropDetails.floorplan_link.name]) < 1:
        #     return False
        elif float(property_listing[rmv_constants.RmvPropDetails.rent_pcm.name]) < 1200:
            return False
        elif not(lower_date_threshold <= available_date <= upper_date_threshold):
            return False
        else:
            return True
    except TypeError:
        pass

def gen_random_user_agent():
    user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
    return random.choice(user_agent_list)


def csv_parser(file, read=True, write=False):
    properties_list = []
    if read:
        with open(file, 'r') as f:
            reader = csv.reader(f)
            temp_list = list(reader)[1:]
            for each in temp_list:
                properties_list += [x for x in each if x != '']
    elif write:
        pass

    return properties_list

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
#
# postcode_list = ["OUTCODE^1685","OUTCODE^1683","OUTCODE^1673","OUTCODE^1666",
#                  "OUTCODE^762","OUTCODE^763","OUTCODE^2791","OUTCODE^2795","OUTCODE^770","OUTCODE^755","OUTCODE^744"]

postcode_list = ["OUTCODE^2510","OUTCODE^2498","OUTCODE^2517","OUTCODE^2522","OUTCODE^2521","OUTCODE^2317",
                 "OUTCODE^2311","OUTCODE^2309","OUTCODE^2316","OUTCODE^749","OUTCODE^744","OUTCODE^750","OUTCODE^756",
                 "OUTCODE^755","OUTCODE^744","OUTCODE^755","OUTCODE^763","OUTCODE^755","OUTCODE^762","OUTCODE^745","OUTCODE^758",
                 "OUTCODE^752","OUTCODE^758","OUTCODE^762","OUTCODE^1673","OUTCODE^1672","OUTCODE^1674","OUTCODE^1680","OUTCODE^1686",
                 "OUTCODE^1682","OUTCODE^1683","OUTCODE^770","OUTCODE^2795","OUTCODE^2791","OUTCODE^1666","OUTCODE^1683","OUTCODE^1685",
                 "OUTCODE^1676","OUTCODE^1861","OUTCODE^1855","OUTCODE^1859","OUTCODE^1857"]

keywords = 'parking,garden'

# for postcode in postcode_list:
#     print("Searching through postcode identifier {} of {} postcodes".format(postcode, len(postcode_list)))
#     property_id_list = get_properties_summary(rmv_constants.BASE_URL + 'find.html', postcode, radius=0.5,
#                                               maxPrice=1750, minBedrooms=1, keywords=keywords)
#     for prop in property_id_list:
#         get_property_details(prop, keywords.split(','))


properties_list = csv_parser('listings_emily_second_pass_date_available_min_rent.csv', read=True)

for property in properties_list:
    get_property_details(property, keywords.split(','))

# [get_property_details(property_id) for property_id in property_id_list]
# results = get_soup_webpage(url_summary.format(0))
# print(results)

# get_property_details('property-76133335')
# get_property_details('property-45850637')
# get_property_details('property-43602508')
# get_property_details('property-47191512')
# test = dataclasses.fields(rmv_constants.RmvPropDetails)

# if __name__ == "__main__":
#    pass
