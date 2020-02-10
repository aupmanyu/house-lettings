import multiprocessing as mp
import timeit
import csv
import math
import urllib3
import random
import requests
import datetime
import dataclasses
import json
from functools import partial
from calmjs.parse import es5
from calmjs.parse.asttypes import Object, VarDecl, FunctionCall, Arguments, Assign, GetPropAssign, PropIdentifier
from calmjs.parse.walkers import Walker
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from bs4 import BeautifulSoup

import rmv_constants
import util


# "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=POSTCODE%5E1218949&radius=0.5""

class RmvScraper:

    def __init__(self):
        self.base_url = rmv_constants.BASE_URL
        self.search_url = self.base_url + rmv_constants.SEARCH_URI
        self.max_results_per_page = rmv_constants.MAX_RESULTS_PER_PAGE

    def search_parallel(self, search_postcodes: [str], **kwargs):
        search_partial = partial(self.search_summary, **kwargs)
        prop_details_partial = partial(self._get_property_details, **kwargs)
        with mp.get_context("spawn").Pool(processes=15) as pool:
            properties_id_list = pool.map(search_partial, search_postcodes)
            properties_id_list_flat = [item for sublist in properties_id_list for item in sublist]
            # results = pool.starmap(self.search, search_postcodes, **kwargs)
            print("Got back {} results and getting their profiles now".format(len(properties_id_list_flat)))
            # print(properties_id_list_flat)
            property_profiles = pool.map(prop_details_partial, properties_id_list_flat)
            property_profiles = list(filter(lambda x: True if x is not None else False, property_profiles))
            csv_parser(property_profiles, "write")
            print("Got back profiles for {} properties".format(len(property_profiles)))
            # print(property_profiles)

    def search_summary(self, search_postcode: str, **kwargs):
        print("Searching through postcode {}".format(search_postcode))
        properties_id_list = []
        total_results = self._get_total_results(self.search_url, search_postcode, **kwargs)
        index_for_pages = [self.max_results_per_page * i for
                           i in range(0, math.ceil(total_results / self.max_results_per_page))]

        for index in index_for_pages:
            properties_id_list.extend(self._get_properties_summary(self.search_url, search_postcode,
                                                              index=index, **kwargs))
        return properties_id_list


    def _get_total_results(self, url: str, postcode_identifier: str, **kwargs):
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        xpath_total_count = rmv_constants.TOTAL_COUNT_FILTER
        payload = {
            "locationIdentifier": postcode_identifier.replace(' ', ''),
            "radius": kwargs['radius'] if 'radius' in kwargs else 0,
            "minBedrooms": kwargs['minBedrooms'] if 'minBedrooms' in kwargs else None,
            "maxPrice": kwargs['maxPrice'] if 'maxPrice' in kwargs else None,
            "keywords": kwargs['keywords'] if 'keywords' in kwargs else None
        }
        data = requests.get(url, headers=headers, params=payload)

        if data.status_code == 200:
            soup = BeautifulSoup(data.text, "html.parser")
            total_count = int((soup.find("span", xpath_total_count)).contents[0])
            return total_count


    def _get_properties_summary(self, url: str, postcode_identifier: str, index=None, **kwargs):
        """
        Gets the summary page from Rightmove and filters by xpath_property_card HTML div
        to get to the Rightmove-specific unique IDs for each property
        """
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }
        xpath_property_card = rmv_constants.PROPERTY_ID_FILTER
        properties_id_list = []
        payload = {
            "locationIdentifier": postcode_identifier.replace(' ', ''),
            "radius": kwargs['radius'] if 'radius' in kwargs else 0,
            "index": index,
            "minBedrooms": kwargs['minBedrooms'] if 'minBedrooms' in kwargs else None,
            "maxPrice": kwargs['maxPrice'] if 'maxPrice' in kwargs else None,
            "keywords": kwargs['keywords'] if 'keywords' in kwargs else None
        }

        try:
            data = util.requests_retry_session().get(url, headers=headers, params=payload)
            if data.status_code == 200:
                soup = BeautifulSoup(data.text, "html.parser")
                properties_soup = soup.find_all("div", xpath_property_card)
                for prop in properties_soup:
                    properties_id_list.append(prop.get('id'))

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(url, e))
            pass

        # print("Total results returned are {} so making {} more iterations".format(total_count, iterations - 1))
        # print("Iteration {}".format(iterations))
        return properties_id_list

    def _get_property_details(self, property_id: str, **kwargs):
        url = self.base_url + property_id + '.html'
        print("Getting details for property URL: {}".format(url))
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        keywords = kwargs['keywords'].split(',') if 'keywords' in kwargs else None

        xpath_description = rmv_constants.PROPERTY_DESCRIPTION_FILTER

        try:
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
            print("Finished parsing property URL {}".format(url))

            if date_available_filter(property_listing, '2020-02-10-00-00-00', '2020-04-01-00-00-00'):
                if enough_images_filter(property_listing, 0):
                    return property_listing

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(url, e))
            pass

        except AttributeError as e:
            print("An error occurred parsing data from url {}: {}".format(url, e))
            pass

        except Exception as e:
            print("Some other error occurred parsing data from url {}: {}".format(url, e))
            pass


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
    if not rmv_constants.RmvPropDetails.image_links.name in property_listing or \
            len(property_listing[rmv_constants.RmvPropDetails.image_links.name]) < threshold:
        return False
    else:
        return True


def floorplan_filter(property_listing):
    if not rmv_constants.RmvPropDetails.floorplan_link.name in property_listing or\
            len(property_listing[rmv_constants.RmvPropDetails.floorplan_link.name]) < 1:
        return False
    else:
        return True


def csv_parser(file, mode=None):
    properties_list = []
    if mode == 'read':
        with open(file, 'r') as f:
            reader = csv.reader(f)
            temp_list = list(reader)[1:]
            for each in temp_list:
                properties_list += [x for x in each if x != '']
    elif mode == 'write':
        with open('michelle.csv', 'w') as f:
            output = csv.writer(f, delimiter=',')
            output.writerow(file[0].keys())
            for row in file:
                output.writerow(row.values())


    return properties_list

# postcode_list = ["OUTCODE^2510","OUTCODE^2498","OUTCODE^2517","OUTCODE^2522","OUTCODE^2521","OUTCODE^2317",
#                  "OUTCODE^2311","OUTCODE^2309","OUTCODE^2316","OUTCODE^749","OUTCODE^744","OUTCODE^750","OUTCODE^756",
#                  "OUTCODE^755","OUTCODE^744","OUTCODE^755","OUTCODE^763","OUTCODE^755","OUTCODE^762","OUTCODE^745","OUTCODE^758",
#                  "OUTCODE^752","OUTCODE^758","OUTCODE^762","OUTCODE^1673","OUTCODE^1672","OUTCODE^1674","OUTCODE^1680","OUTCODE^1686",
#                  "OUTCODE^1682","OUTCODE^1683","OUTCODE^770","OUTCODE^2795","OUTCODE^2791","OUTCODE^1666","OUTCODE^1683","OUTCODE^1685",
#                  "OUTCODE^1676","OUTCODE^1861","OUTCODE^1855","OUTCODE^1859","OUTCODE^1857"]

postcode_list = ["OUTCODE^744","OUTCODE^755", "OUTCODE^1666", "OUTCODE^1685", "OUTCODE^1861", "OUTCODE^2311"]

keywords = ''

# for postcode in postcode_list:
#     print("Searching through postcode identifier {} of {} postcodes".format(postcode, len(postcode_list)))
#     property_id_list = get_properties_summary(rmv_constants.BASE_URL + 'find.html', postcode, radius=0.5,
#                                               maxPrice=1750, minBedrooms=1, keywords=keywords)
#     for prop in property_id_list:
#         get_property_details(prop, keywords.split(','))


# properties_list = csv_parser('listings_emily_second_pass_date_available_min_rent.csv', read=True)

# for property in properties_list:
#     get_property_details(property, keywords.split(','))

# [get_property_details(property_id) for property_id in property_id_list]
# results = get_soup_webpage(url_summary.format(0))
# print(results)

# get_property_details('property-68127093', keywords=None)
# get_property_details('property-45850637')
# get_property_details('property-43602508')
# get_property_details('property-47191512')
# test = dataclasses.fields(rmv_constants.RmvPropDetails)


if __name__ == "__main__":
    # mp.set_start_method('spawn')
    start = timeit.default_timer()
    rmv_properties = RmvScraper()
    rmv_properties.search_parallel(postcode_list, radius=0, maxPrice=2600, minBedrooms=3)
    end = timeit.default_timer()
    print("It took {} seconds to run".format(end - start))

