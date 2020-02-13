import multiprocessing as mp
import math
import urllib3
import requests
from functools import partial
from calmjs.parse import es5
from calmjs.parse.asttypes import Assign
from calmjs.parse.walkers import Walker
from bs4 import BeautifulSoup

import rmv_constants
import util


class RmvScraper:

    def __init__(self):
        self.base_url = rmv_constants.BASE_URL
        self.search_url = self.base_url + rmv_constants.SEARCH_URI
        self.max_results_per_page = rmv_constants.MAX_RESULTS_PER_PAGE

    def search_parallel(self, search_postcodes: [str], **kwargs):
        search_partial = partial(self._search_summary, **kwargs)
        prop_details_partial = partial(self._get_property_details, **kwargs)
        with mp.get_context("spawn").Pool(processes=15) as pool:
            properties_id_list = pool.map(search_partial, search_postcodes)
            properties_id_list_flat = [item for sublist in properties_id_list for item in sublist]
            # results = pool.starmap(self.search, search_postcodes, **kwargs)
            print("Got back {} results and getting their profiles now".format(len(properties_id_list_flat)))
            # print(properties_id_list_flat)
            property_profiles = pool.map(prop_details_partial, properties_id_list_flat)
            property_profiles = list(filter(lambda x: True if x is not None else False, property_profiles))
            print("Got back profiles for {} properties".format(len(property_profiles)))
        return property_profiles

    def _search_summary(self, search_postcode: str, **kwargs):
        print("Searching through postcode {}".format(search_postcode))
        properties_id_list = []
        total_results = self._get_total_results(search_postcode, **kwargs)
        index_for_pages = [self.max_results_per_page * i for
                           i in range(0, math.ceil(total_results / self.max_results_per_page))]

        for index in index_for_pages:
            properties_id_list.extend(self._get_properties_summary(search_postcode,
                                                                   index=index, **kwargs))
        return properties_id_list

    def _get_total_results(self, postcode_identifier: str, **kwargs):
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        xpath_total_count = rmv_constants.TOTAL_COUNT_FILTER
        payload = {
            "locationIdentifier": postcode_identifier.replace(' ', ''),
            "radius": kwargs['radius'] if 'radius' in kwargs else 0,
            "minBedrooms": kwargs['minBedrooms'] if 'minBedrooms' in kwargs else None,
            "maxPrice": kwargs['maxPrice'] if 'maxPrice' in kwargs else None,
            "keywords": ','.join(kwargs['keywords']) if kwargs['keywords'] else None
        }
        data = requests.get(self.search_url, headers=headers, params=payload)

        if data.status_code == 200:
            soup = BeautifulSoup(data.text, "html.parser")
            total_count = int((soup.find("span", xpath_total_count)).contents[0])
            return total_count

    def _get_properties_summary(self, postcode_identifier: str, index=None, **kwargs):
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
            "keywords": ','.join(kwargs['keywords']) if kwargs['keywords'] else None
        }

        try:
            data = util.requests_retry_session().get(self.search_url, headers=headers, params=payload)
            if data.status_code == 200:
                soup = BeautifulSoup(data.text, "html.parser")
                properties_soup = soup.find_all("div", xpath_property_card)
                for prop in properties_soup:
                    properties_id_list.append(prop.get('id'))

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(data.url, e))
            pass

        return properties_id_list

    def _get_property_details(self, property_id: str, **kwargs):
        url = self.base_url + property_id + '.html'
        print("Getting details for property URL: {}".format(url))
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        xpath_description = rmv_constants.PROPERTY_DESCRIPTION_FILTER

        try:
            data = requests.get(url, headers=headers)
            soup = BeautifulSoup(data.text, "html.parser")
            property_listing = {}
            description_text = soup.find("div", xpath_description).text
            property_listing[rmv_constants.RmvPropDetails.description.name] = description_text.strip().replace('\n', '')

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
            return property_listing

            # if date_available_filter(property_listing, '2020-02-04-00-00-00', '2020-04-01-00-00-00'):
            #     if enough_images_filter(property_listing, 0):
            #         return property_listing

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(url, e))
            pass

        except AttributeError as e:
            print("An error occurred parsing data from url {}: {}".format(url, e))
            pass

        except Exception as e:
            print("Some other error occurred parsing data from url {}: {}".format(url, e))
            pass

