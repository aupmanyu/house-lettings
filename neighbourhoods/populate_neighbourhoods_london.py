import csv
import uuid
import requests
import timeit
from bs4 import BeautifulSoup
import psycopg2.extras

import general_constants

psycopg2.extras.register_uuid()


def get_london_nhoods():
    gln_timer_start = timeit.default_timer()
    print("Getting all neighbourhoods in London ...")
    r = requests.get(general_constants.NEIGHBOURHOODS_URL)

    if r.status_code == 200:
        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        london_nhoods = []
        for items in soup.find('table', class_='wikitable sortable').find_all('tr')[1::]:
            data = items.find_all(['td'])
            london_nhoods.append({"region": data[0].text})
            # london_nhoods.append((data[0].text, data[1].next_element.string, data[3].text, data[5].text.strip()))
    gln_timer_end = timeit.default_timer()
    print("Getting all neighbourhoods in London took {} seconds".format(gln_timer_end - gln_timer_start))
    return london_nhoods


def match_london_nhoods(nhoods_wiki: list, nhoods_rmv: list):
    print("Figuring out which neighbourhoods in London also exist on RMV ...")
    nhoods_wiki = [x["region"].lower().split("(")[0].strip() for x in nhoods_wiki]
    nhoods_rmv = [x.lower() for x in nhoods_rmv]

    no_match_london_nhoods = [{"region": x.title()} for x in nhoods_wiki if x not in nhoods_rmv]

    return no_match_london_nhoods


def in_london(nhood: str, nhoods_london: list):
    nhoods_london = [x.lower().split("(")[0].strip() for x in nhoods_london]
    return nhood.lower() in nhoods_london


def standardise_nhoods_sql(nhoods: dict, london_nhoods: list):
    return {
        "nhood_uuid": uuid.uuid4(),
        "nhood_name": nhoods.get("region"),
        "rmv_id": nhoods.get("region_code"),
        "polyline": nhoods.get("polyline"),
        "in_london": in_london(nhoods["region"], london_nhoods)
    }


if __name__ == '__main__':
    timer_start = timeit.default_timer()
    london_nhoods_wiki = get_london_nhoods()
    nhoods_rmv = []
    with open('rmv_region_polyline_mapping.csv', 'r') as f:
        print("Reading in regions in RMV with polylines ...")
        reader = csv.DictReader(f)
        for row in reader:
            nhoods_rmv.append(row)

    no_match_nhoods = match_london_nhoods(london_nhoods_wiki, [x['region'] for x in nhoods_rmv])

    print("Standardising all nhoods now ...")
    std_timer_start = timeit.default_timer()
    standardised_nhoods = [standardise_nhoods_sql(x, [list(x.values())[0] for x in london_nhoods_wiki]) for x in nhoods_rmv]
    [standardised_nhoods.append(
        standardise_nhoods_sql(x, [list(x.values())[0] for x in london_nhoods_wiki])) for x in no_match_nhoods]
    std_timer_stop = timeit.default_timer()

    print("Standardising all listings took {} seconds".format(std_timer_stop - std_timer_start))

    nhoods_populate_query_many = """
    INSERT INTO nhoods_uk
    (nhood_id, nhood_name, rmv_id, polyline, in_london)
     VALUES %s
    """

    print("Writing data to DB ...")
    db_timer_start = timeit.default_timer()
    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            psycopg2.extras.execute_values(curs, nhoods_populate_query_many,
                                           [tuple(x.values()) for x in standardised_nhoods])

    db_timer_end = timeit.default_timer()
    print("Finished writing to DB in {} seconds".format(db_timer_end - db_timer_start))

    timer_end = timeit.default_timer()

    print("Finished and it took {} seconds".format(timer_end - timer_start))
