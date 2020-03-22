import re
import uuid
import json
import datetime
import dateutil.parser as parser
import multiprocessing as mp
import math
import urllib3

import requests
import psycopg2
import psycopg2.extras
import psycopg2.errors
from calmjs.parse import es5
from calmjs.parse.asttypes import Assign, UnaryExpr
from calmjs.parse.walkers import Walker
from bs4 import BeautifulSoup

import general_constants
import rmv_constants
import util


class RmvScraper:

    def __init__(self, config):
        self.base_url = rmv_constants.BASE_URL
        self.find_url = self.base_url + rmv_constants.FIND_URI
        self.bounding_area_url = rmv_constants.SEARCH_URL
        self.max_results_per_page = rmv_constants.MAX_RESULTS_PER_PAGE
        self.outcode_list = None
        self._parse_config(config)
        with open('rmv_outcode_lookup.json') as f:
            self._outcode_lookup = json.load(f)

    def search_parallel(self):
        self._get_search_areas()
        with mp.get_context("spawn").Pool(processes=15) as pool:
            properties_id_list = pool.map(self._search_summary, self.outcode_list)
            properties_id_list_flat = [item for sublist in properties_id_list for item in sublist]
            # results = pool.starmap(self.search, search_postcodes, **kwargs)
            print("Got back {} results and getting their profiles now".format(len(properties_id_list_flat)))
            # print(properties_id_list_flat)
            property_profiles = pool.map(self._get_property_details, properties_id_list_flat)
            property_profiles = list(filter(lambda x: True if x is not None else False, property_profiles))
            print("Got back profiles for {} properties".format(len(property_profiles)))

        [self._insert_to_db(x) for x in property_profiles]
        print("Finished storing all listings in DB")
        return property_profiles

    def _parse_config(self, config):
        try:
            self.destinations = config['destinations']
            self.max_price = int(config['maxPrice'])
            self.min_bedrooms = config['minBedrooms']
            self.keywords = config['keywords']
            self.radius = config['radius']
        except KeyError as e:
            raise e

    def _get_search_areas(self):
        print("Geocoding user's destinations ...")
        [self.destinations[i][k].update({"geocode": util.geocode_address(k)})
         for i, x in enumerate(self.destinations) for k, v in x.items()]

        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        payload = {
            "criteria": {
                "price": self.max_price,
                "bedrooms": self.min_bedrooms,
                "propertyType": "ALL",
                "transactionType": 2  # constant as 2 is for rent
            },
            "poiLocations": []
        }

        for each in self._gen_pois(self.destinations):
            payload['poiLocations'].append(each)

        print("Calculating which areas meet user's needs ...")
        r = requests.post(self.bounding_area_url, headers=headers, json=payload)

        if r.status_code == 200:
            postcode_list = [x["outcode"] for x in json.loads(r.content)["outcodes"]]
            print("Found {} areas that meet user's needs".format(len(postcode_list)))
            print("Converting postcodes to outcodes ...")
            with open('outcode_mappings_not_found.txt', 'a+') as f:
                self.outcode_list = ["OUTCODE^" + str(self._outcode_lookup[postcode])
                                     if postcode in self._outcode_lookup else json.dump(postcode + '\n', f)
                                     for postcode in postcode_list]

                # remove any None resulting from mappings not found
                self.outcode_list = [x for x in self.outcode_list if x is not None]
        else:
            raise requests.exceptions.HTTPError(
                "An error occurred getting postcodes with error code {}. Error Message: {}"
                .format(r.status_code, json.loads(r.content)))

    @staticmethod
    def _gen_pois(pois):
        count = 0
        for poi in pois:
            for k, v in poi.items():
                for mode in v['modes'][0]:
                    count += 1
                    rmv_poi = {
                        "poiId": count,
                        "travelType": rmv_constants.RmvTransportModes[mode].value.split(','),
                        "location": {
                            "lat": v['geocode'][0],
                            "lng": v['geocode'][1]
                        },
                        "travelTime": v['modes'][0][mode] * 60,
                        "placeType": "W"
                    }
                    yield rmv_poi

    def _search_summary(self, search_postcode: str):
        print("Searching through postcode {}".format(search_postcode))
        properties_id_list = []
        total_results = self._get_total_results(search_postcode)
        index_for_pages = [self.max_results_per_page * i for
                           i in range(0, math.ceil(total_results / self.max_results_per_page))]

        for index in index_for_pages:
            properties_id_list.extend(self._get_properties_summary(search_postcode, index=index))
        return properties_id_list

    def _get_total_results(self, postcode_identifier: str):
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        xpath_total_count = rmv_constants.TOTAL_COUNT_FILTER
        payload = {
            "locationIdentifier": postcode_identifier.replace(' ', ''),
            "radius": self.radius,
            "minBedrooms": self.min_bedrooms,
            "maxPrice": self.max_price,
            "keywords": ','.join(self.keywords)
        }
        data = requests.get(self.find_url, headers=headers, params=payload)

        if data.status_code == 200:
            soup = BeautifulSoup(data.text, "html.parser")
            total_count = int((soup.find("span", xpath_total_count)).contents[0])
            return total_count

    def _get_properties_summary(self, postcode_identifier: str, index=None):
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
            "radius": self.radius,
            "index": index,
            "minBedrooms": self.min_bedrooms,
            "maxPrice": self.max_price,
            "keywords": ','.join(self.keywords)
        }

        try:
            data = util.requests_retry_session().get(self.find_url, headers=headers, params=payload)
            if data.status_code == 200:
                soup = BeautifulSoup(data.text, "html.parser")
                properties_soup = soup.find_all("div", xpath_property_card)
                for prop in properties_soup:
                    properties_id_list.append(prop.get('id'))

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(data.url, e))
            pass

        return properties_id_list

    def _get_property_details(self, property_id: str):
        url = self.base_url + '/' + property_id + '.html'
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
            property_listing[rmv_constants.RmvPropDetails.description.name] = description_text.strip().replace('\n',
                                                                                                               ' ')

            scripts_soup = soup.find_all('script')
            scripts_with_details = list(
                filter(lambda x: True if x.find(rmv_constants.PROPERTY_DETAILS_FILTER) >= 0 else False,
                       [str(scripts_soup[y].next).strip().replace('\r', '')
                       .replace('\n', '')
                       .replace('\t', '')
                        for y in range(0, len(scripts_soup))]))

            # the field we want is repeated many times in this script so we just pick one
            # (use 6th element because it's first occurrence of clean JS code that doesn't cause the parser to break)
            scripts_with_availability = \
                list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_AVAILABILITY_FILTER) >= 0 else False,
                            [str(scripts_soup[y].next).strip().replace('\r', '')
                            .replace('\n', '')
                            .replace('\t', '')
                             for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')[6]

            # hacks because there is a JS error in this script (missing semicolon) further down (around char 10710)
            # that causes parser to break
            try:
                scripts_with_images = \
                    list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_IMAGES_FILTER) >= 0 else False,
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
                scripts_with_floorplans = \
                    list(filter(lambda x: True if x.find(rmv_constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                                [str(scripts_soup[y].next).strip().replace('\r', '')
                                .replace('\n', '')
                                .replace('\t', '')
                                 for y in range(0, len(scripts_soup))]))[0].split('(jQuery);')
                scripts_with_floorplans = list(
                    filter(lambda x: True if x.find(rmv_constants.PROPERTY_FLOORPLAN_FILTER) >= 0 else False,
                           [y for y in scripts_with_floorplans]))
            except IndexError:
                scripts_with_floorplans = []

            # scripts_with_availability made list with [] because it is string above because of .split()[6] indexing
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
                            elif field.name == rmv_constants.RmvPropDetails.floorplan_links.name:
                                property_listing[field.name] = [link.value.replace('"', '') for link in
                                                                node.right.items]
                            elif field.name == rmv_constants.RmvPropDetails.date_available.name:
                                property_listing[field.name] = datetime.datetime.strftime(
                                    datetime.datetime.strptime(str(node.right.value).replace('"', ''),
                                                               "%Y-%m-%d-%H-%M-%S"), "%Y-%m-%d %H:%M:%S")
                            else:
                                if isinstance(node.right, UnaryExpr):
                                    # node.right.value.value because float() only takes str or number
                                    # but not type "Number" which is what UnaryExpr type contains for node.right.value
                                    property_listing[field.name] = str(float(node.right.value.value) * -1) \
                                        if node.right.op == '-' else str(node.right.value)
                                else:
                                    property_listing[field.name] = str(node.right.value).replace('"', '')
                            break

            match = re.search(r'(\d+/\d+/\d+)', description_text)
            if match:
                property_listing[rmv_constants.RmvPropDetails.date_available.name] = \
                    datetime.datetime.strftime(parser.parse(match.group(1)), "%Y-%m-%d %H:%M:%S")
            print("Finished parsing property URL {}".format(url))
            self._standardise_listing(property_listing)
            return property_listing

            # if date_available_filter(property_listing, '2020-02-04-00-00-00', '2020-04-01-00-00-00'):
            #     if enough_images_filter(property_listing, 0):
            #         return property_listing

        except (TimeoutError, urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError) as e:
            print("An error occurred getting url {}: {}".format(url, e))
            pass

        except AttributeError as e:
            print("An error occurred parsing data from url {}: {}. CULPRIT: {}".format(url, e, property_listing))
            pass

        except Exception as e:
            print(
                "Some other error occurred parsing data from url {}: {}. CULPRIT: {}".format(url, e, property_listing))
            pass

    def _standardise_listing(self, property_profile: dict):
        self._add_uuid_listing(property_profile)
        self._add_prop_url(property_profile)
        desired_keys = set(map(lambda x: x.name, rmv_constants.RmvPropDetails))
        existing_keys = set(property_profile.keys())
        non_existing_keys = desired_keys - existing_keys
        for key in non_existing_keys:
            property_profile[key] = None
        return property_profile

    @staticmethod
    def _add_uuid_listing(property_profile: dict):
        property_profile[rmv_constants.RmvPropDetails.prop_uuid.name] = util.gen_uuid()

    @staticmethod
    def _add_prop_url(property_profile: dict):
        property_profile[rmv_constants.RmvPropDetails.url.name] = util.rmv_generate_url_from_id(property_profile)

    def _insert_to_db(self, property_profile: dict):
        insert_string = """
        INSERT INTO property_listings
        (prop_uuid, geo_lat, geo_long, postcode, rent_pcm,
        beds, date_available, website_unique_id, image_links,
        floorplan_links, estate_agent, estate_agent_address, description, url, date_written_to_db) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """

        print("Storing property with UUID {} and RMV ID {} in DB now ...".
              format(property_profile[rmv_constants.RmvPropDetails.prop_uuid.name],
                     property_profile[rmv_constants.RmvPropDetails.rmv_unique_link.name]))

        psycopg2.extras.register_uuid()
        with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(insert_string,
                                 (property_profile[rmv_constants.RmvPropDetails.prop_uuid.name],
                                  property_profile[rmv_constants.RmvPropDetails.geo_lat.name],
                                  property_profile[rmv_constants.RmvPropDetails.geo_long.name],
                                  property_profile[rmv_constants.RmvPropDetails.postcode.name],
                                  property_profile[rmv_constants.RmvPropDetails.rent_pcm.name],
                                  property_profile[rmv_constants.RmvPropDetails.beds.name],
                                  property_profile[rmv_constants.RmvPropDetails.date_available.name],
                                  property_profile[rmv_constants.RmvPropDetails.rmv_unique_link.name],
                                  json.dumps(property_profile[rmv_constants.RmvPropDetails.image_links.name]),
                                  json.dumps(property_profile[rmv_constants.RmvPropDetails.floorplan_links.name]),
                                  property_profile[rmv_constants.RmvPropDetails.estate_agent.name],
                                  property_profile[rmv_constants.RmvPropDetails.estate_agent_address.name],
                                  property_profile[rmv_constants.RmvPropDetails.description.name],
                                  property_profile[rmv_constants.RmvPropDetails.url.name],
                                  datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
                                  ))
                except Exception as e:
                    print("Error occurred storing property in DB: {}. CULPRIT OBJECT: {} ".
                          format(e, property_profile))
