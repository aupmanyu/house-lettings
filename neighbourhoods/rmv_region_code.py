import os
import json
import traceback
import multiprocessing as mp
from time import sleep
import csv
import timeit
import random
from bs4 import BeautifulSoup

import util
import rmv_constants

POLYLINE = os.environ.get('POLYLINE')

start_time = timeit.default_timer()

potential_codes = [i for i in range(0, 100000)]

xpath = {"class": "input input--full"}


def read_mapped_codes(file):
    mapped_codes = {}
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapped_codes.update({row['region_code']: row['region']})

    return mapped_codes


def generate_codes(code: int):
    url = rmv_constants.BASE_URL + rmv_constants.FIND_URI
    lap_timer_start = timeit.default_timer()
    sleep(random.uniform(2, 8))
    mapping = {}
    try:
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        params = {
            "locationIdentifier": f"REGION^{code:05d}"
        }

        print("Trying region code {}".format(code))
        r = util.requests_retry_session().get(url, headers=headers, params=params)

        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            region = soup.find("input", xpath).attrs['value']
            mapping[code] = region
            # codes.remove(region_code)

        elif r.status_code == 404 or r.status_code == 400:
            mapping[code] = None
            # codes.remove(code)

        lap_timer_stop = timeit.default_timer()
        print("Iteration took {} seconds".format(lap_timer_stop - lap_timer_start))

    except ConnectionError as e:
        print("Request for region code {} failed: {}".format(code, e))

    except AttributeError as e:
        print("Could not parse soup: {}. CULPRIT: {}".format(e, soup))
        sleep(3*60)

    except TypeError as e:
        print("The code passed was not in list format: {}. CULPRIT: {}".format(e, code))

    except Exception as e:
        print("Some other error occurred: {}".format(e))

    return mapping


def get_polyline(region_code: int):
    print("Trying region code {}".format(region_code))
    mapping_polyline = {}
    url = rmv_constants.ROOT_URL + '/api' + '/_mapSearch'
    sleep(random.uniform(2, 7))

    try:
        headers = {
            'User-Agent': util.gen_random_user_agent()
        }

        params = {
            "locationIdentifier": f"REGION^{region_code:05d}",
            "numberOfPropertiesPerPage": 499,
            "radius": 0.0,
            "sortType": 6,
            "index": 0,
            "viewType": "MAP",
            "channel": "RENT",
            "areaSizeUnit": "sqft",
            "currencyCode": "GBP",
            "isFetching": "false",
            "viewport": "-0.272832,0.0550416,51.4883,51.6052"
        }

        r = util.requests_retry_session().get(url, headers=headers, params=params)

        if r.status_code == 200:
            mapping_polyline[region_code] = json.loads(r.content, encoding='utf-8')['locationPolygon']

        elif r.status_code == 400:
            mapping_polyline[region_code] = None

        else:
            print("Something went wrong for code {}. Error message: {}".format(region_code, r.content))

    except Exception as e:
        print("An exception occurred ...: {}".format(e))
        sleep(3*60)

    return mapping_polyline


if __name__ == '__main__':
    file_code_mapping = 'rmv_region_mapping.csv'
    file_polyline = 'rmv_region_polyline_mapping.csv'

    if POLYLINE:
        try:
            polyline_mapping = []
            region_codes_mapping = read_mapped_codes(file_code_mapping)
            non_null_codes = [int(k) for k, v in region_codes_mapping.items() if v]

            polyline_mapped_codes = []

            with open(file_polyline, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                        polyline_mapped_codes.append(int(row['region_code']))

            non_mapped_polyline_codes = set(non_null_codes) - set(polyline_mapped_codes)

            pool = mp.Pool(processes=10)
            for result in pool.imap_unordered(get_polyline, non_mapped_polyline_codes, chunksize=20):
                polyline_mapping.append({
                    "region_code": list(result.keys())[0],
                    "region": region_codes_mapping[str(list(result.keys())[0])],
                    "polyline": list(result.values())[0]
                })

            with open(file_polyline, 'a') as f:
                writer = csv.DictWriter(f, ['region_code', 'region', 'polyline'], delimiter=',')
                # writer.writeheader()
                for each in polyline_mapping:
                    writer.writerow(each)

        except KeyboardInterrupt:
            print("Got keyboard interrupt ... saving progress so far to file")
            with open(file_polyline, 'a') as f:
                writer = csv.DictWriter(f, ['region_code', 'region', 'polyline'], delimiter=',')
                # writer.writeheader()
                for each in polyline_mapping:
                    writer.writerow(each)

        except Exception as e:
            print(traceback.format_exc())
            print("Writing everything so far to file ...")
            with open(file_polyline, 'a') as f:
                writer = csv.DictWriter(f, ['region_code', 'region', 'polyline'], delimiter=',')
                # writer.writeheader()
                for each in polyline_mapping:
                    writer.writerow(each)
            raise e

    else:
        consolidate_mapping = {}
        try:
            mapped_codes = map(lambda x: int(x), read_mapped_codes(file_code_mapping).keys())
            remaining_codes = list(set(potential_codes) - set(mapped_codes))
            print("Need to go through {} codes".format(len(remaining_codes)))

            pool = mp.Pool(processes=25)
            for each in pool.imap_unordered(generate_codes, list(remaining_codes), chunksize=100):
                consolidate_mapping.update(each)

            with open(file_code_mapping, 'a') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerows(consolidate_mapping.items())

        except KeyboardInterrupt as e:
            print("Got keyboard interrupt ... saving progress so far to file")
            with open(file_code_mapping, 'a') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerows(consolidate_mapping.items())

        except Exception as e:
            print("An error occurred: {} ... Writing everything so far to file".format(e))
            with open(file_code_mapping, 'a') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerows(consolidate_mapping.items())

    end_time = timeit.default_timer()
    print("Finished and it took {} seconds".format(end_time - start_time))
