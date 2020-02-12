import datetime
import timeit
import json
from functools import partial

from rmv_scraper import RmvScraper

import util
import filters

USER = 'michelle'
NEW_RUN = False

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

        output_file = USER_OUTPUT_DATA_PATH + '{}_{}.csv'.format(USER, util.time_now())
        print("Creating a backup of returned data in file {} located at {}".format(output_file, USER_OUTPUT_DATA_PATH))
        util.csv_writer(rmv_properties, output_file)

        with open(USER_OUTPUT_DATA_PATH + '.lastrun', 'w') as f:
            f.write(output_file)

    else:
        with open(USER_OUTPUT_DATA_PATH + '.lastrun', 'r') as f:
            rmv_properties = util.csv_reader(f.read())

    filters = [partial(filters.enough_images_filter, threshold=4),
               partial(filters.date_available_filter, lower_threshold='2020-02-06-00-00-00',
                       upper_threshold='2020-04-01-00-00-00'),
               partial(filters.min_rent_filter, threshold=1200)]
    filtered_properties = list(filter(lambda x: all(f(x) for f in filters), rmv_properties))
    print("Filtered properties: {}".format(filtered_properties))
