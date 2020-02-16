import datetime
import timeit
import json
from functools import partial
from typing import List, Any

from rmv_scraper import RmvScraper

import util
import filters
import rmv_constants

USER = 'michelle'
NEW_RUN = False

# ------------------------- // -------------------------

USER_CONFIG_PATH = './users/{}/input/user_config.json'.format(USER)
USER_OUTPUT_DATA_PATH = './users/{}/output/'.format(USER)

try:
    print("Trying to open user {} config file from this location {}".format(USER, USER_CONFIG_PATH))
    with open(USER_CONFIG_PATH, 'r') as data_file:
        config = json.load(data_file)
except FileNotFoundError:
    raise("Unable to find a config file for user {} at this location: {}. "
          "Please make sure the file exists at the right location before running the code again"
          .format(USER, USER_CONFIG_PATH))

if __name__ == "__main__":
    if NEW_RUN:
        print("This is a new run so going to the Internet to get deets ...")
        start = timeit.default_timer()
        rmv = RmvScraper()
        rmv_properties = rmv.search_parallel(config['postcode_list'], radius=config['radius'],
                                             maxPrice=config['maxPrice'],
                                             minBedrooms=config['minBedrooms'], keywords=config['keywords'])
        end = timeit.default_timer()
        print("It took {} seconds to get back all deets from the Internet".format(end - start))

        output_file_all_data = USER_OUTPUT_DATA_PATH + '{}_{}.csv'.format(USER, util.time_now())
        util.csv_writer(rmv_properties, output_file_all_data)
        print("Created a backup of returned deets in file {}".format(output_file_all_data))

        with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'w') as f:
            f.write(output_file_all_data)

    else:
        with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'r') as f:
            backup_file = f.read()
            print("This is a re-run so reading deets from backup file: {}".format(backup_file))
            rmv_properties = util.csv_reader(backup_file)

    filters = [partial(filters.enough_images_filter, threshold=4),
               partial(filters.date_available_filter, lower_threshold='2020-02-11-00-00-00',
                       upper_threshold='2020-04-01-00-00-00'),
               partial(filters.min_rent_filter, threshold=1200)]

    print("Filtering properties now ...")
    filtered_properties = list(filter(lambda x: all(f(x) for f in filters), rmv_properties))
    print("Retained {} properties after filtering".format(len(filtered_properties)))

    output_file_filtered = USER_OUTPUT_DATA_PATH + '{}_{}_filtered.csv'.format(USER, util.time_now())

    try:
        print("Identifying any duplicates from previous runs ...")
        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'r') as f:
            unique_properties = []
            prev_filtered_properties_id = set([x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                           for file in f.read().split('\n') if file for x in util.csv_reader(file)])
            curr_filtered_properties_id = set(x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                                 for x in filtered_properties)
            unique_properties_id = curr_filtered_properties_id.difference(prev_filtered_properties_id)

            print("Removed {} duplicates from previous runs"
                  .format(len(filtered_properties) - len(unique_properties_id)))

            for each in filtered_properties:
                for k,v in each.items():
                    if k == rmv_constants.RmvPropDetails.rmv_unique_link.name:
                        if v in unique_properties_id:
                            unique_properties.append(each)
                        break

            if unique_properties:
                util.csv_writer(unique_properties, output_file_filtered)
                print("FINAL RESULT: {} properties stored at {}".format(len(unique_properties), output_file_filtered))
            else:
                print("FINAL RESULT: No new properties so not writing to a file. Quitting now!")

        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'a') as f:
            f.write('{}\n'.format(output_file_filtered))

    except FileNotFoundError:
        util.csv_writer(filtered_properties, output_file_filtered)
        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'a') as f:
            f.write('{}\n'.format(output_file_filtered))
        print("No previous runs founds so no duplicates need to be removed")
        print("FINAL RESULT: {} properties stored at {}".format(len(filtered_properties), output_file_filtered))


