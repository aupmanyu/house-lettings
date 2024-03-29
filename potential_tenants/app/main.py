import os
import sys
import json
from time import sleep
import uuid
import errno
import random
import timeit
import datetime
import traceback
from functools import partial

import requests
import psycopg2
import psycopg2.extras
import psycopg2.errors

from app.rmv_scraper import RmvScraper

from app import util, filters, general_constants, ranking, rmv_constants
from travel import travel_time

DEBUG = os.environ.get("DEBUG").lower() == 'true' or False
MAX_USER_RESULTS = int(os.environ.get("MAX_USER_RESULTS"))

USER = 'test_user'
NEW_RUN = True

WEBFLOW_COLLECTION_ID = "5e62aadc51beef34cfbc64d8"

# ------------------------- // -------------------------

USER_CONFIG_PATH = './users/{}/input/user_config.json'.format(USER)
USER_OUTPUT_DATA_PATH = './users/{}/output/'.format(USER)
USER_ALL_CACHE_FILE = USER_OUTPUT_DATA_PATH + '.lastrunall'
USER_FILTER_CACHE_FILE = USER_OUTPUT_DATA_PATH + '.lastrunfiltered'


def new_search(config):
    print("This is a new run so going to the Internet to get deets ...")
    start = timeit.default_timer()
    rmv = RmvScraper(config)
    try:
        rmv_properties = rmv.search_parallel()

    except KeyError as e:
        print("The config file is malformed: {} does not exist".format(e))
        exit()

    end = timeit.default_timer()
    print("It took {} seconds to get back all deets from the Internet".format(end - start))

    return rmv_properties


def get_prev_filtered_props_id(user_uuid: uuid.UUID):
    get_prev_results_query = """
        SELECT website_unique_id, score FROM filtered_properties 
        WHERE user_uuid = %s
        """

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            curs.execute(get_prev_results_query, (user_uuid,))
            prev_filtered_properties = {x[0]: x[1] for x in curs.fetchall()}

    return prev_filtered_properties


def remove_duplicates(user_uuid: uuid.UUID, curr_properties_list: list):
    print("Identifying any properties from previous runs that have the same scores so that these can be discarded ...")

    try:
        indexed_curr_properties = {x[rmv_constants.RmvPropDetails.rmv_unique_link.name]: x for x in
                                   curr_properties_list}

        prev_filtered_properties_id_score = get_prev_filtered_props_id(user_uuid)
        curr_filtered_properties_id_score = {x[rmv_constants.RmvPropDetails.rmv_unique_link.name]: x["score"]
                                             for x in curr_properties_list}
        duplicate_properties_id = []

        for k, v in curr_filtered_properties_id_score.items():
            if k in prev_filtered_properties_id_score and v == prev_filtered_properties_id_score[k]:
                duplicate_properties_id.append(k)

    except (ValueError, KeyError):
        print(traceback.format_exc(), file=sys.stderr)

    # This is because sometimes code goes through the same RMV ID twice possibly because RMV returns same property
    # for different areas. This is guaranteed positive or 0 since indexed object will be unique
    # through dict construction and set is unique
    curr_properties_duplicates = len(curr_properties_list) - len(indexed_curr_properties)

    # duplicate_properties_id = curr_filtered_properties_id.intersection(prev_filtered_properties_id)
    unique_properties = []
    for x in indexed_curr_properties:
        try:
            if x not in duplicate_properties_id:
                unique_properties.append(indexed_curr_properties[x])
        # unique_properties = [list(x.values())[0] for x in indexed_curr_properties_list
        #                  if x not in duplicate_properties_id]
        except Exception:
            print(traceback.format_exc(), file=sys.stderr)
            print("Remove duplicates - CULPRIT: {}".format(x))
        continue

    # for each in indexed_curr_properties_list:
    #     if list(each.keys())[0] not in duplicate_properties_id:
    #         unique_properties.append(list(each.values())[0])
    #
    # for prop_id in duplicate_properties_id:
    #     for each in indexed_curr_properties_list:
    #         if prop_id in each.keys():
    #             indexed_curr_properties_list.pop()

    # for i, each in enumerate(curr_properties_list):
    #     for k, v in each.items():
    #         if k == rmv_constants.RmvPropDetails.rmv_unique_link.name:
    #             if v in duplicate_properties_id:
    #                 del curr_properties_list[i]
    #             break

    print("Removed {} duplicates from previous runs".format(len(duplicate_properties_id) + curr_properties_duplicates))

    return unique_properties


def upsert_user_db(user_config):
    psycopg2.extras.register_uuid()
    user_uuid = util.gen_uuid()

    insert_user_query = """
       INSERT INTO users 
       (user_uuid, email, max_rent, min_beds, keywords, destinations, date_low, date_high, desired_cats, desired_nhoods, 
       webflow_form_number)
       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
       ON CONFLICT (email)
       DO UPDATE
        SET max_rent = EXCLUDED.max_rent,
            min_beds = EXCLUDED.min_beds,
            keywords = EXCLUDED.keywords,
            destinations = EXCLUDED.destinations,
            date_low = EXCLUDED.date_low,
            date_high = EXCLUDED.date_high,
            desired_cats = EXCLUDED.desired_cats,
            desired_nhoods = EXCLUDED.desired_nhoods,
            webflow_form_number = EXCLUDED.webflow_form_number
       RETURNING (user_uuid)
       """

    insert_user_transaction_query = """
    INSERT INTO user_transactions
    (user_uuid, insert_timestamp, payload)
    VALUES (%s, %s, %s)
    """

    user = {
        "user_uuid": str(user_uuid),
        "email": user_config['email'],
        "maxPrice": user_config["maxPrice"],
        "minBedrooms": user_config["minBedrooms"],
        "keywords": ','.join([x.name for x in user_config['keywords']]),
        "destinations": json.dumps(user_config['destinations']),
        "date_low": user_config['date_low'],
        "date_high": user_config['date_high'],
        "desired_cats": ','.join([x.name for x in user_config['desired_cats']]),
        "desired_nhoods": ','.join([x for x in user_config['desired_areas']]),
        "webflow_form_number": user_config["webflow_form_number"]
    }

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            curs.execute(insert_user_query, tuple([*user.values()]))
            user_uuid = curs.fetchone()[0]
            curs.execute(insert_user_transaction_query,
                         (user_uuid,
                          datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"),
                          json.dumps(user)))

    print("Stored/updated user details with email {} in DB. UUID is {}".format(user_config['email'], user_uuid))

    return user_uuid


def standardise_filtered_listing(user_uuid: uuid.UUID, filtered_listing: dict):
    return {
        "user_uuid": user_uuid,
        "prop_uuid": filtered_listing['prop_uuid'],
        "website_unique_id": filtered_listing[rmv_constants.RmvPropDetails.rmv_unique_link.name],
        "url": filtered_listing[rmv_constants.RmvPropDetails.url.name],
        "date_sent_to_user": datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"),
        "avg_travel_time_transit": filtered_listing['avg_travel_time_transit'],
        "avg_travel_time_walking": filtered_listing['avg_travel_time_walking'],
        "avg_travel_time_bicycling": filtered_listing["avg_travel_time_bicycling"],
        "avg_travel_time_driving": filtered_listing["avg_travel_time_driving"],
        "augment": json.dumps(filtered_listing['augment']),
        "score": filtered_listing["score"]
    }


def get_webflow_users():
    url = "https://api.webflow.com/collections/5e9cb6cb572a494febd4efb3/items"

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "accept-version": "1.0.0",
        "Content-Type": "application/json"
    }

    user_mapping = {}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()

        for each in data['items']:
            user_mapping[int(each["name"])] = each["_id"]

    except requests.exceptions.HTTPError as e:
        raise e

    return user_mapping


def get_tube_stops_cms_items():
    url = "https://api.webflow.com/collections/5eaf0803a0d3e484ca69b0db/items"

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "accept-version": "1.0.0",
        "Content-Type": "application/json"
    }

    r = requests.get(url, headers=headers)

    tube_stop_collection_id_mapping = {}

    if r.status_code == 200:
        data = r.json()
        for each in data['items']:
            tube_stop_collection_id_mapping[each['name']] = each['_id']
        total_results = data['total']
        if total_results > data["count"]:
            iters = int(total_results / data["count"])
            for i in range(1, iters + 1):
                r = requests.get(url, headers=headers, params={"offset": i * 100})
                if r.status_code == 200:
                    data = r.json()
                    for each in data['items']:
                        tube_stop_collection_id_mapping[each['name']] = each['_id']

    return tube_stop_collection_id_mapping


def write_webflow_cms(final_properties_list, webflow_user_mapping, user_config):
    tube_stop_collection_id_mapping = get_tube_stops_cms_items()

    webflow_db_mapping_query = """
    INSERT INTO properties_cms_mapping 
    (prop_uuid, webflow_cms_id)
    VALUES (%s, %s)
    """

    image_indices = random.sample(
        [x for x in range(0, len(final_properties_list[rmv_constants.RmvPropDetails.image_links.name]))], 4)

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "accept-version": "1.0.0",
        "Content-Type": "application/json"
    }

    payload = {
        "fields": {
            "_archived": False,
            "_draft": False,
            "name": final_properties_list[rmv_constants.RmvPropDetails.rmv_unique_link.name],
            "slug": str(final_properties_list[rmv_constants.RmvPropDetails.prop_uuid.name]),
            "full-address": final_properties_list[rmv_constants.RmvPropDetails.street_address.name],
            "original-ad-link": final_properties_list[rmv_constants.RmvPropDetails.url.name],
            "rent-pcm": round(float(final_properties_list[rmv_constants.RmvPropDetails.rent_pcm.name]), 2),
            "bedrooms": int(final_properties_list[rmv_constants.RmvPropDetails.beds.name]),
            "main-image": final_properties_list[rmv_constants.RmvPropDetails.image_links.name][image_indices[0]],
            "image-2": final_properties_list[rmv_constants.RmvPropDetails.image_links.name][image_indices[1]],
            "image-3": final_properties_list[rmv_constants.RmvPropDetails.image_links.name][image_indices[2]],
            "image-4": final_properties_list[rmv_constants.RmvPropDetails.image_links.name][image_indices[3]],
            "score": final_properties_list['score'],
            "user-email": user_config['email'],
            "user-email-2": webflow_user_mapping[user_config["webflow_form_number"]],
            "tube-stop": []
        }
    }

    for stop in final_properties_list["augment"]["nearby_station_zones"]:
        tube_stop = list(stop.keys())[0] + " " + "Underground Station"
        try:
            if tube_stop in tube_stop_collection_id_mapping:
                payload["fields"]["tube-stop"].append(tube_stop_collection_id_mapping[tube_stop])

        except KeyError:
            pass

    for i, each in enumerate(user_config['destinations']):
        dest = list(each.keys())[0]
        modes = [list(x.keys())[0] for x in each[dest]['modes']]
        commute_strings = []
        for mode in modes:
            commute_strings.append("{}min {}".
                                   format(int(final_properties_list['augment']['travel_time'][i][dest][mode]), mode))

        final_commute_string = ', '.join(commute_strings).replace('transit', 'public transport')

        payload["fields"]["commute-{}".format(i + 1)] = "{}: {}".format(dest, final_commute_string).capitalize()

    url = "https://api.webflow.com/collections/{}/items?live=true".format(WEBFLOW_COLLECTION_ID)

    r = requests.post(url, headers=headers, json=payload)

    if r.status_code == 200:
        print("Successfully added property ID {} in CMS".
              format(final_properties_list[rmv_constants.RmvPropDetails.rmv_unique_link.name]))
        content = json.loads(r.content)

        with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
            with conn.cursor() as curs:
                curs.execute(webflow_db_mapping_query,
                             (final_properties_list[rmv_constants.RmvPropDetails.prop_uuid.name], content['_id']))
        try:
            if int(r.headers['X-RateLimit-Remaining']) <= 1:  # 1 instead of 0 because of bug in Webflow API
                print("Going to sleep for 70s to reset Webflow rate limit ...")
                sleep(70)  # Sleep for 60s before making new requests to Webflow
        except KeyError:
            pass

    else:
        # TODO: the error occurs when rent-pcm = inf. Need to fix this so these props don't make it through the pipeline
        print("An error occurred for property ID {} writing to CMS: {}".format(
            final_properties_list[rmv_constants.RmvPropDetails.rmv_unique_link.name], r.content))
        print("CULPRIT: {}".format(payload))


def update_prop_status(prop_id, status):
    get_cms_item_id_query = """
    SELECT webflow_cms_id FROM properties_cms_mapping 
    WHERE prop_uuid = %s
    """

    update_property_status_query = """
    UPDATE filtered_properties 
    SET user_favourites = %s
    WHERE prop_uuid = %s
    RETURNING website_unique_id
    """

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            curs.execute(get_cms_item_id_query, (prop_id,))
            cms_item_id = curs.fetchone()
            curs.execute(update_property_status_query, (status, prop_id))
            website_id = curs.fetchone()[0]
            print("Updated status of property {} in DB to {}".format(website_id, status))

    url = "https://api.webflow.com/collections/{}/items/{}?live=true".format(WEBFLOW_COLLECTION_ID, cms_item_id[0])

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "accept-version": "1.0.0",
        "Content-Type": "application/json"
    }

    payload = {
        "fields": {
            "user-rating": status
        }
    }
    r = requests.patch(url, headers=headers, json=payload)

    if r.status_code == 200:
        print("Updated status of property {} in CMS to {}".format(website_id, status))
    else:
        print("An error occurred updating status of property {} in CMS: {}".format(website_id, json.loads(r.content)))


def main(config):
    user_uuid = upsert_user_db(config)
    if NEW_RUN:
        rmv_properties = new_search(config)
    else:
        try:
            with open(USER_ALL_CACHE_FILE, 'r') as f:
                backup_file = f.read()
                print("This is a re-run so reading deets from backup file: {}".format(backup_file))
                rmv_properties = util.csv_reader(backup_file)
        except FileNotFoundError as e:
            print("{}. Quitting now ...".format(e))
            exit(errno.ENOENT)

    # Filtering properties
    lower_threshold = datetime.datetime.strptime(config['date_low'], "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=5)
    upper_threshold = datetime.datetime.strptime(config['date_high'], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=5)

    images_threshold = 4
    min_rent_factor = 0.55

    properties_to_filter = rmv_properties
    while True:
        print("Filtering criteria: Images Threshold: {}. Min Rent Factor: {}".format(images_threshold, min_rent_factor))
        filters_to_use = [partial(filters.enough_images_filter, threshold=images_threshold),
                          partial(filters.date_available_filter,
                                  lower_threshold=datetime.datetime.strftime(lower_threshold, "%Y-%m-%d %H:%M:%S"),
                                  upper_threshold=datetime.datetime.strftime(upper_threshold, "%Y-%m-%d %H:%M:%S")),
                          partial(filters.min_rent_filter, threshold=min_rent_factor * config['maxPrice'])]

        print("Filtering properties now ...")
        for i, f in enumerate(filters_to_use):
            filtered_properties = [x for x in properties_to_filter if f(x)]
            print("Step {} Filter: Removed {} properties".format(i, len(properties_to_filter) - len(filtered_properties)))
            properties_to_filter = filtered_properties

        # filtered_properties = list(filter(lambda x: all(f(x) for f in filters_to_use), rmv_properties))
        print("Retained {} properties after filtering".format(len(filtered_properties)))

        if len(filtered_properties) <= MAX_USER_RESULTS:
            break

        if images_threshold < 6:
            images_threshold += 1

        if min_rent_factor < 0.8:
            min_rent_factor += 0.05

        if images_threshold > 6 or min_rent_factor > 0.8:
            break

        print("Updating filtering criteria ...")

    insert_many_filtered_prop_query = """
    INSERT into filtered_properties 
    (user_uuid, prop_uuid, website_unique_id, url, date_sent_to_user, 
    avg_travel_time_transit, avg_travel_time_walking, 
    avg_travel_time_bicycling, avg_travel_time_driving,
    augment, score)
    VALUES %s
    ON CONFLICT (user_uuid, website_unique_id)
    DO NOTHING
    """

    insert_many_zone_address_query = """
    UPDATE property_listings
    SET (street_address, zone_best_guess) = (data.street_address, data.zone_best_guess)
    FROM (VALUES %s) AS data(street_address, zone_best_guess, prop_uuid)
    WHERE property_listings.prop_uuid = data.prop_uuid
    """

    # Score properties before removing duplicates incase scoring has changed
    property_scorer = ranking.PropertyScorer()
    print("Scoring properties now ...")
    [x.update(
        {"score": property_scorer.score(x, config['desired_areas'], config['desired_cats'], config['keywords'])})
        for x in filtered_properties]

    # Remove duplicates
    filtered_properties = remove_duplicates(user_uuid, filtered_properties)

    if filtered_properties:
        sorted_filtered_properties = sorted(filtered_properties, key=lambda k: k['score'], reverse=True)
        top_results = sorted_filtered_properties[:MAX_USER_RESULTS]
        # no list comprehension needed for commute times because GMAPS API can take in 25 x 25 (origins x destinations)
        print("Getting travel times and zones for properties now ...")
        travel_time.get_commute_times(top_results, [k for x in config['destinations'] for k in x.keys()])
        [travel_time.get_property_zone(x) for x in top_results]
        standardised_filtered_listing = [standardise_filtered_listing(user_uuid, x) for x in top_results]

        try:
            with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
                with conn.cursor() as curs:
                    # template = "%(user_uuid)s,%(prop_uuid)s,%(website_unique_id)s,%(url)s," \
                    #            "%(date_sent_to_user)s,%(avg_travel_time_transit)s," \
                    #            "%(avg_travel_time_walking)s,%(avg_travel_time_bicycling)s," \
                    #            "%(avg_travel_time_driving)s,%(augment)s,%(score)s"
                    # print(curs.mogrify(template, standardised_filtered_listing[0]))
                    psycopg2.extras.execute_values(curs, insert_many_filtered_prop_query,
                                                   [tuple(x.values()) for x in standardised_filtered_listing],
                                                   template=None)
                    template = "(%s::varchar, %s::int, %s)"
                    psycopg2.extras.execute_values(curs, insert_many_zone_address_query,
                                                   [(x[rmv_constants.RmvPropDetails.street_address.name],
                                                     x[rmv_constants.RmvPropDetails.zone_best_guess.name],
                                                     x[rmv_constants.RmvPropDetails.prop_uuid.name])
                                                    for x in top_results], template=template)
                    print("Stored {} new filtered properties in DB.".format(len(standardised_filtered_listing)))
                    if not DEBUG:
                        user_mapping = get_webflow_users()
                        [write_webflow_cms(x, user_mapping, config) for x in top_results]
                    else:
                        print("Skipping writing to Webflow because DEBUG is {}".format(DEBUG))

        except Exception as e:
            print("Could not store some properties in DB: {}".format(e))
            print(traceback.format_exc(), file=sys.stderr)

        print("All done now! Thanks for running!")

    else:
        print("FINAL RESULT: No new properties so not writing to DB. Thanks for running!")


if __name__ == '__main__':
    psycopg2.extras.register_uuid()
    # filtered_properties = {
    #     'description': "Letting information:Date available:NowFurnishing:FurnishedLetting type:Long termReduced on "
    #                    "Rightmove: 13 March 2020 (28 minutes ago)Key featuresDouble BedroomsBalcony24 Hour "
    #                    "ConciergeCommunal Roof TerraceResidents RoomCommunal GardensCommunal 24hr GymFull description "
    #                    "       This is a stunning apartment within the South Gardens development, the first of the "
    #                    "wider Elephant Park scheme. The apartment is set in the Baldwin Point tower.This apartment "
    #                    "comprises of two double bedrooms, a bathroom an open plan reception with a fitted kitchen "
    #                    "with Bosch appliances including a washer dryer and balcony.  The apartment is finished to a "
    #                    "high internal specification including oak engineered wood flooring and underfloor heating "
    #                    "throughout. Other benefits of the building include a 24 hour concierge, communal gardens and "
    #                    "a communal roof terrace. South Gardens is perfectly located for transport links to the City, "
    #                    "the West End and beyond with a range of local bus routes, the tube and National Rail "
    #                    "services. The development features a residents gym, Communal Gardens, 24 hour concierge, "
    #                    "communal residents room and communal roof terrace.More information from this agentTo view "
    #                    "this media, please visit the on-line version of this page at "
    #                    "www.rightmove.co.uk/property-to-rent/property-67134849.html?premiumA=trueParticularsEnergy "
    #                    "Performance Certificate (EPC) graphsView EPC Rating Graph for this propertySee full size "
    #                    "version online",
    #     'postcode': 'SE17 1AF',
    #     'geo_lat': '51.491705786609934',
    #     'geo_long': '-0.08592939071585458',
    #     'rmv_unique_link': '67134849',
    #     'rent_pcm': '2296.6666666666665',
    #     'beds': '2',
    #     'estate_agent': 'Gordon & Co', 'estate_agent_address': 'Strata Pavillion, 4 Walworth Road, London, SE1 6EB',
    #     'date_available': '2020-03-13 12:57:10',
    #     'image_links': [
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_01_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_02_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_03_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_04_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_05_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_06_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_07_0000_max_656x437.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_IMG_08_0000_max_656x437.jpg'],
    #     'floorplan_links': [
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_FLP_01_0000_max_600x600.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_FLP_01_0000_max_900x900.jpg',
    #         'https://media.rightmove.co.uk/dir/70k/69202/67134849/69202_ELE170658_L_FLP_01_0000_max_1350x1350.jpg'],
    #     'prop_uuid': '428bb0b2-852c-49df-9114-df660fee4622',
    #     'url': 'https://www.rightmove.co.uk/property-to-rent/property-67134849.html',
    #     'zone_best_guess': 1,
    #     'street_address': '1 Townsend St, London SE17 1HY, UK',
    #     'augment': {
    #         'travel_time': [
    #             {
    #                 'EC1R 0EB': {
    #                 'transit': 38.2,
    #                 'walking': 57.666666666666664,
    #                 'bicycling': 19.216666666666665,
    #                 'driving': 20.183333333333334
    #                 }
    #             },
    #             {
    #                 'soho': {
    #                 'transit': 30.0,
    #                 'walking': 66.48333333333333,
    #                 'bicycling': 21.933333333333334,
    #                 'driving': 24.8
    #                       }
    #              }],
    #         'nearby_station_zones': [{'Borough': '1'}]},
    #     'avg_travel_time_transit': 34.1,
    #     'avg_travel_time_walking': 62.075,
    #     'avg_travel_time_bicycling': 20.575,
    #     'avg_travel_time_driving': 22.491666666666667
    # }
    DEBUG = True
    try:
        print("Trying to open user {} config file from this location {}".format(USER, USER_CONFIG_PATH))
        with open(USER_CONFIG_PATH, 'r') as data_file:
            config = json.load(data_file)
    except FileNotFoundError:
        print("Unable to find a config file for user {} at this location: {}. "
              "Please make sure the file exists at the right location before running the code again"
              .format(USER, USER_CONFIG_PATH))
        exit(errno.ENOENT)

    get_tube_stops_cms_items()
    main(config)
