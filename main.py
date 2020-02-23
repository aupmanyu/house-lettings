import errno
import datetime
import timeit
import json
from functools import partial
from typing import List, Any

from rmv_scraper import RmvScraper

import util
import filters
import travel_time
import rmv_constants

USER = 'michelle'
NEW_RUN = False

# ------------------------- // -------------------------

USER_CONFIG_PATH = './users/{}/input/user_config.json'.format(USER)
USER_OUTPUT_DATA_PATH = './users/{}/output/'.format(USER)
USER_ALL_CACHE_FILE = USER_OUTPUT_DATA_PATH + '.lastrunall'
USER_FILTER_CACHE_FILE = USER_OUTPUT_DATA_PATH + '.lastrunfiltered'


def new_run():
    print("This is a new run so going to the Internet to get deets ...")
    start = timeit.default_timer()
    rmv = RmvScraper()
    rmv_properties = rmv.search_parallel(config['postcode_list'], radius=config['radius'],
                                         maxPrice=config['maxPrice'],
                                         minBedrooms=config['minBedrooms'], keywords=config['keywords'])
    end = timeit.default_timer()
    print("It took {} seconds to get back all deets from the Internet".format(end - start))

    output_file_all_data = USER_OUTPUT_DATA_PATH + '{}_{}.csv'.format(USER, util.time_now())
    [util.rmv_generate_url_from_id(x) for x in rmv_properties]
    util.csv_writer(rmv_properties, output_file_all_data)
    print("Created a backup of returned deets in file {}".format(output_file_all_data))

    with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'w') as f:
        f.write(output_file_all_data)
        print("Updated cache file with backup location for faster access next time!")

    return rmv_properties


def remove_duplicates(cache_file: str, curr_properties_list: list):
    print("Identifying any duplicates from previous runs ...")
    try:
        with open(cache_file, 'r') as f:
            prev_filtered_properties_id = set([x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                               for file in f.read().split('\n') if file for x in util.csv_reader(file)])

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
    except FileNotFoundError:
        return curr_properties_list


if __name__ == "__main__":
    try:
        print("Trying to open user {} config file from this location {}".format(USER, USER_CONFIG_PATH))
        with open(USER_CONFIG_PATH, 'r') as data_file:
            config = json.load(data_file)
    except FileNotFoundError:
        print("Unable to find a config file for user {} at this location: {}. "
              "Please make sure the file exists at the right location before running the code again"
              .format(USER, USER_CONFIG_PATH))
        exit(errno.ENOENT)

    if NEW_RUN:
        rmv_properties = new_run()
    else:
        with open(USER_ALL_CACHE_FILE, 'r') as f:
            backup_file = f.read()
            print("This is a re-run so reading deets from backup file: {}".format(backup_file))
            rmv_properties = util.csv_reader(backup_file)

    # Filtering properties
    filters = [partial(filters.enough_images_filter, threshold=4),
               partial(filters.date_available_filter, lower_threshold='2020-02-17-00-00-00',
                       upper_threshold='2020-04-01-00-00-00'),
               partial(filters.min_rent_filter, threshold=1200)]

    print("Filtering properties now ...")
    filtered_properties = list(filter(lambda x: all(f(x) for f in filters), rmv_properties))
    print("Retained {} properties after filtering".format(len(filtered_properties)))

    # Remove duplicates
    filtered_properties = remove_duplicates(USER_FILTER_CACHE_FILE, filtered_properties)

    output_file_filtered = USER_OUTPUT_DATA_PATH + '{}_{}_filtered.csv'.format(USER, util.time_now())

    if filtered_properties:
        travel_time.get_commute_times(filtered_properties, config['destinations'])
        [travel_time.get_property_zone(x) for x in filtered_properties]
        [util.rmv_generate_url_from_id(x) for x in filtered_properties]
        util.csv_writer(filtered_properties, output_file_filtered)
        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'a') as f:
            f.write('{}\n'.format(output_file_filtered))
        print("FINAL RESULT: {} properties stored at {}".format(len(filtered_properties), output_file_filtered))
    else:
        print("FINAL RESULT: No new properties so not writing to a file. Thanks for running!")
