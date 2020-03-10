import uuid
import errno
import datetime
import timeit
import json
from functools import partial

import psycopg2
import psycopg2.extras

from rmv_scraper import RmvScraper

import util
import filters
import travel_time
import rmv_constants

USER = 'test_user'
NEW_RUN = True

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

    ##### FOR DEBUGGING #####

    output_file_all_data = USER_OUTPUT_DATA_PATH + '{}_{}.csv'.format(USER, util.time_now())
    util.csv_writer(rmv_properties, output_file_all_data)
    print("Created a backup of returned deets in file {}".format(output_file_all_data))

    with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'w') as f:
        f.write(output_file_all_data)
        print("Updated cache file with backup location for faster access next time!")

    return rmv_properties


def get_current_filtered_props_id(user_uuid: uuid.UUID):
    get_prev_results_query = """
        SELECT website_unique_id FROM filtered_properties 
        WHERE user_uuid = %s
        """

    with psycopg2.connect(dbname="Aashish", user="Aashish") as conn:
        with conn.cursor() as curs:
            curs.execute(get_prev_results_query, (user_uuid,))
            prev_filtered_properties = [x[0] for x in curs.fetchall()]

    return prev_filtered_properties


def remove_duplicates(user_uuid: uuid.UUID, curr_properties_list: list):
    print("Identifying any duplicates from previous runs ...")

    prev_filtered_properties_id = set(get_current_filtered_props_id(user_uuid))
    curr_filtered_properties_id = set(x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                      for x in curr_properties_list)
    duplicate_properties_id = curr_filtered_properties_id.intersection(prev_filtered_properties_id)

    for i, each in enumerate(curr_properties_list):
        for k, v in each.items():
            if k == rmv_constants.RmvPropDetails.rmv_unique_link.name:
                if v in duplicate_properties_id:
                    del curr_properties_list[i]
                break

    print("Removed {} duplicates from previous runs".format(len(duplicate_properties_id)))

    return curr_properties_list


def upsert_user_db(config):
    psycopg2.extras.register_uuid()
    user_uuid = util.gen_uuid()

    insert_user_command = """
       INSERT INTO users 
       (user_uuid, email, max_rent, min_beds, keywords, destinations)
       VALUES (%s, %s, %s, %s, %s, %s)
       ON CONFLICT (email)
       DO UPDATE
        SET max_rent = EXCLUDED.max_rent,
            min_beds = EXCLUDED.min_beds,
            keywords = EXCLUDED.keywords,
            destinations = EXCLUDED.destinations
       RETURNING (user_uuid)
       """

    with psycopg2.connect(dbname="Aashish", user="Aashish") as conn:
        with conn.cursor() as curs:
            curs.execute(insert_user_command,
                         (user_uuid,
                         config['email'],
                         config['maxPrice'],
                         config['minBedrooms'],
                         ','.join(config['keywords']),
                         json.dumps(config['destinations'])))
            user_uuid = curs.fetchone()[0]

    print("Stored/updated user details with email {} in DB. UUID is {}".format(config['email'], user_uuid))

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
        "augment": json.dumps(filtered_listing['augment'])
    }


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
    filters_to_use = [partial(filters.enough_images_filter, threshold=4),
                      partial(filters.date_available_filter, lower_threshold='2020-03-01 00:00:00',
                              upper_threshold='2020-04-01 00:00:00'),
                      partial(filters.min_rent_filter, threshold=1500)]

    print("Filtering properties now ...")
    filtered_properties = list(filter(lambda x: all(f(x) for f in filters_to_use), rmv_properties))
    print("Retained {} properties after filtering".format(len(filtered_properties)))

    insert_many_filtered_prop_query = """
    INSERT into filtered_properties 
    (user_uuid, prop_uuid, website_unique_id, url, date_sent_to_user, 
    avg_travel_time_transit, avg_travel_time_walking, 
    avg_travel_time_bicycling, avg_travel_time_driving,
    augment)
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

    # Remove duplicates
    filtered_properties = remove_duplicates(user_uuid, filtered_properties)

    if filtered_properties:
        # no list comprehension needed for commute times because GMAPS API can take in 25 x 25 (origins x destinations)
        travel_time.get_commute_times(filtered_properties, [k for x in config['destinations'] for k in x.keys()])
        [travel_time.get_property_zone(x) for x in filtered_properties]

        standardised_filtered_listing = [standardise_filtered_listing(user_uuid, x) for x in filtered_properties]

        with psycopg2.connect(dbname="Aashish", user="Aashish") as conn:
            with conn.cursor() as curs:
                template = "%(user_uuid)s,%(prop_uuid)s,%(website_unique_id)s,%(url)s," \
                           "%(date_sent_to_user)s,%(avg_travel_time_transit)s," \
                           "%(avg_travel_time_walking)s,%(avg_travel_time_bicycling)s," \
                           "%(avg_travel_time_driving)s,%(augment)s"
                # print(curs.mogrify(template, standardised_filtered_listing[0]))
                psycopg2.extras.execute_values(curs, insert_many_filtered_prop_query,
                                               [tuple(x.values()) for x in standardised_filtered_listing],
                                               template=None)

                psycopg2.extras.execute_values(curs, insert_many_zone_address_query,
                                               [(x[rmv_constants.RmvPropDetails.street_address.name],
                                                x[rmv_constants.RmvPropDetails.zone_best_guess.name],
                                                x[rmv_constants.RmvPropDetails.prop_uuid.name]) for x in filtered_properties])

        print("Stored {} new filtered properties in DB".format(len(standardised_filtered_listing)))

    else:
        print("FINAL RESULT: No new properties so not writing to a file. Thanks for running!")


if __name__ == '__main__':
    try:
        print("Trying to open user {} config file from this location {}".format(USER, USER_CONFIG_PATH))
        with open(USER_CONFIG_PATH, 'r') as data_file:
            config = json.load(data_file)
    except FileNotFoundError:
        print("Unable to find a config file for user {} at this location: {}. "
              "Please make sure the file exists at the right location before running the code again"
              .format(USER, USER_CONFIG_PATH))
        exit(errno.ENOENT)
    main(config)
