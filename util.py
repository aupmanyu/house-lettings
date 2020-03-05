import os
import csv
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

import googlemaps

import rmv_constants

gmaps_key = os.environ['GMAPS_KEY']
gmaps = googlemaps.Client(key=gmaps_key)


def time_now():
    return datetime.datetime.strftime(datetime.datetime.now(), "%Y_%m_%d_%H_%M_%S")


def validate_postcode(postcode: str):
    try:
        res = requests.get('http://api.getthedata.com/postcode/' + postcode.replace(' ', ''))
        if res.status_code == 200:
            if res.json()["status"] != "match":
                raise ValueError("The postcode entered is incorrect")
            else:
                return True
    except ValueError:
        raise ValueError("The postcode entered is incorrect")
    except Exception as e:
        raise ConnectionError("An error occurred while verifying postcode with 3rd party service: {}".format(e))


def find_value_nested_dict(input_dict: dict, value: str):
    for k, v in input_dict.items():
        if isinstance(v, dict):
            find_value_nested_dict(v, value)
        elif isinstance(v, list):
            [find_value_nested_dict(item, value) for item in v]
        else:
            if k == "@type" and v == value:
                print(input_dict)
                break


def gen_random_user_agent():
    user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
    return random.choice(user_agent_list)


def requests_retry_session(retries=6, backoff_factor=0.6, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

    properties_list = []
    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            properties_list.append(dict(row))

    return properties_list


def csv_reader(file):
    '''

    :param file: input csv file to read
    :return: a list containing a dict for each property
    '''

    properties_list = []
    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            properties_list.append(dict(row))

    return properties_list


def csv_writer(data, out_file):
    max_fields = 0
    max_field_dict = None
    for each in data:
        if len(each.keys()) > max_fields:
            max_fields = len(each.keys())
            max_field_dict = each

    fieldnames = [x for x in max_field_dict.keys()]

    with open(out_file, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        for each in data:
            writer.writerow(each)


def rmv_generate_url_from_id(property_dict: [dict]):
    property_dict['url'] = rmv_constants.BASE_URL + '/' \
                           + 'property-{}'.format(property_dict[rmv_constants.RmvPropDetails.rmv_unique_link.name]) \
                           + '.html'


def chunks(lst, n):
    """Yield successive indices for n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        if i + n <= len(lst):
            yield i, i + n
        else:
            yield i, len(lst)


def geocode_address(location: str):
    r = gmaps.geocode(location, region='uk')
    return tuple((r[0]['geometry']['location']).values())
