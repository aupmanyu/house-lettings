import csv
import uuid
import requests
import traceback
import timeit
from bs4 import BeautifulSoup
import psycopg2
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


def write_db_london_nhoods_cats(file):
    get_london_nhood_id_query = """
    SELECT DISTINCT ON (nhood_name) nhood_id, nhood_name FROM nhoods_uk
    WHERE in_london is TRUE
    """
    insert_nhood_cat_query = """
    INSERT INTO nhoods_cat 
    (nhood_id, best, beautiful, luxurious, nightlife, eating, restaurants, shopping, walk, green, village, 
    young_professional, students, family, artsy, nhood_id)
    VALUES %s
    """
    london_nhoods_cat = []
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            london_nhoods_cat.append(row)

    london_nhoods_id = {}
    with psycopg2.connect(general_constants.DB_URL, sslmode="allow") as conn:
        with conn.cursor() as curs:
            curs.execute(get_london_nhood_id_query)
            for x in curs.fetchall():
                london_nhoods_id[x[1].lower()] = x[0]

    try:
        for each in london_nhoods_cat:
            each.update({"nhood_id": london_nhoods_id[each['Location'].lower()]})
            each.pop("Location")
            for k, v in each.items():
                if not v:
                    each[k] = 0

    except KeyError as e:
        print("A key couldn't be found ...: {}".format(e))
        print(traceback.print_exc())
        pass

    with psycopg2.connect(general_constants.DB_URL, sslmode="allow") as conn:
        with conn.cursor() as curs:
            template = "(%(Best)s, %(Beautiful)s, %(Luxurious)s, %(Nightlife)s, %(Eating)s, %(Restaurants)s, " \
                       "%(Shopping)s, %(Walk)s, %(Green)s, %(Village)s, %(Young professional)s, %(Students)s, " \
                       "%(Family)s, %(Artsy)s, %(nhood_id)s)"
            psycopg2.extras.execute_values(curs, insert_nhood_cat_query, london_nhoods_cat,
                                           template=template)


if __name__ == '__main__':
    timer_start = timeit.default_timer()

    # ------------ Populate nhoods_uk DB: should give 52611 records ---------------
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

    # ------------ Populate nhoods_uk DB: end ---------------

    print("Storing London neighbourhoods categorisations now ...")
    write_db_london_nhoods_cats("london_nhood_cats.csv")

    timer_end = timeit.default_timer()

    print("Finished and it took {} seconds".format(timer_end - timer_start))
