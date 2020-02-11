import datetime
import timeit
import json
from rmv_scraper import RmvScraper

import util

USER = 'michelle'

# ------------------------- // -------------------------

USER_CONFIG_PATH = './users/{}/input/user_config.json'.format(USER)
USER_OUTPUT_DATA_PATH = './users/{}/output/'.format(USER)

with open(USER_CONFIG_PATH, 'r') as data_file:
    config = json.load(data_file)

if __name__ == "__main__":
    start = timeit.default_timer()
    rmv = RmvScraper()
    rmv_properties = rmv.search_parallel(config['postcode_list'], radius=config['radius'], maxPrice=config['maxPrice'],
                                         minBedrooms=config['minBedrooms'], keywords=config['keywords'])
    end = timeit.default_timer()
    print("It took {} seconds to run".format(end - start))

    output_file = '{}_{}.csv'.format(USER, util.time_now())
    print("Creating a backup of returned data in file {} located at {}".format(output_file, USER_OUTPUT_DATA_PATH))
    util.csv_writer(rmv_properties, USER_OUTPUT_DATA_PATH + output_file)