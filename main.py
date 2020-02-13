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
NEW_RUN = True

# ------------------------- // -------------------------

USER_CONFIG_PATH = './users/{}/input/user_config.json'.format(USER)
USER_OUTPUT_DATA_PATH = './users/{}/output/'.format(USER)

with open(USER_CONFIG_PATH, 'r') as data_file:
    config = json.load(data_file)

if __name__ == "__main__":
    if NEW_RUN:
        start = timeit.default_timer()
        rmv = RmvScraper()
        rmv_properties = rmv.search_parallel(config['postcode_list'], radius=config['radius'],
                                             maxPrice=config['maxPrice'],
                                             minBedrooms=config['minBedrooms'], keywords=config['keywords'])
        end = timeit.default_timer()
        print("It took {} seconds to run".format(end - start))

        output_file_all_data = USER_OUTPUT_DATA_PATH + '{}_{}.csv'.format(USER, util.time_now())
        print("Creating a backup of returned data in file {} located at {}".format(output_file_all_data, USER_OUTPUT_DATA_PATH))
        util.csv_writer(rmv_properties, output_file_all_data)

        with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'w') as f:
            f.write(output_file_all_data)

    else:
        with open(USER_OUTPUT_DATA_PATH + '.lastrunall', 'r') as f:
            rmv_properties = util.csv_reader(f.read())

    filters = [partial(filters.enough_images_filter, threshold=4),
               partial(filters.date_available_filter, lower_threshold='2020-02-11-00-00-00',
                       upper_threshold='2020-04-01-00-00-00'),
               partial(filters.min_rent_filter, threshold=1200)]

    filtered_properties = list(filter(lambda x: all(f(x) for f in filters), rmv_properties))

    output_file_filtered = USER_OUTPUT_DATA_PATH + '{}_{}_filtered.csv'.format(USER, util.time_now())
    try:
        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'r') as f:
            unique_properties = []
            prev_filtered_properties_id = set(x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                              for x in util.csv_reader(f.read()))
            curr_filtered_properties_id = set(x[rmv_constants.RmvPropDetails.rmv_unique_link.name]
                                                 for x in filtered_properties)
            unique_properties_id = curr_filtered_properties_id.difference(prev_filtered_properties_id)
            for each in filtered_properties:
                for k,v in each.items():
                    if k == rmv_constants.RmvPropDetails.rmv_unique_link.name:
                        if v in unique_properties_id:
                            unique_properties.append(each)
                        break
            util.csv_writer(unique_properties, output_file_filtered)
            print("Found {} properties after filtering stored at {}".format(len(unique_properties),
                                                                            output_file_filtered))
    except FileNotFoundError:
        util.csv_writer(filtered_properties, output_file_filtered)
        with open(USER_OUTPUT_DATA_PATH + '.lastrunfiltered', 'w') as f:
            f.write(output_file_filtered)
        print("Found {} properties after filtering stored at {}".format(len(filtered_properties), output_file_filtered))


