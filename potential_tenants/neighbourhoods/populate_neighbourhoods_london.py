import os
import csv
import uuid
import requests
import polyline
import traceback
import timeit
from enum import Enum
from time import sleep
from shapely import geometry
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

from app import general_constants

psycopg2.extras.register_uuid()


class Steps(Enum):
    UK_NHOODS_DB_POPULATE = 1
    CLEAN_DATA = 2
    WRITE_WEBFLOW = 3
    LONDON_CATS_NHOODS_DB_POPULATE = 4


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
    SELECT nhood_id, nhood_name FROM nhoods_uk
    WHERE in_london is TRUE
    """
    insert_nhood_cat_query = """
    INSERT INTO nhoods_cat 
    (best, beautiful, luxurious, nightlife, eating, restaurants, shopping, walk, green, village, 
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

    remove_list = []
    for each in london_nhoods_cat:
        try:
            each.update({"nhood_id": london_nhoods_id[each['Location'].lower()]})
            each.pop("Location")
            for k, v in each.items():
                if not v:
                    each[k] = 0

        except KeyError as e:
            print("A key couldn't be found so removing from list ...: {}".format(e))
            remove_list.append(each)
        continue

    [london_nhoods_cat.remove(each) for each in remove_list]

    with psycopg2.connect(general_constants.DB_URL, sslmode="allow") as conn:
        with conn.cursor() as curs:
            template = "(%(Best)s, %(Beautiful)s, %(Luxurious)s, %(Nightlife)s, %(Eating)s, %(Restaurants)s, " \
                       "%(Shopping)s, %(Walk)s, %(Green)s, %(Village)s, %(Young professional)s, %(Students)s, " \
                       "%(Family)s, %(Artsy)s, %(nhood_id)s)"
            psycopg2.extras.execute_values(curs, insert_nhood_cat_query, london_nhoods_cat,
                                           template=template)


def clean_data():
    '''
    There are neighbourhoods elsewhere in UK that have the same names as neighbourhoods in London.
    This function attempts to identify those neighbourhoods and tag them with in_london = FALSE in DB
    '''

    get_existing_london_areas_query = """
    SELECT nhood_id, nhood_name, polyline nhood_name
    FROM nhoods_uk
    WHERE in_london is TRUE
    """

    update_area_status_query = """
    UPDATE nhoods_uk
    SET in_london = FALSE
    WHERE nhood_id in %s
    """

    greater_london_polyline = "wccyHnkcBg^gEoK_Xwj@gEg^_Xod@oKgEwQwQgEgEg^gEfE_XgEoK_XwQ?wQf^wQ?oKnK_" \
                              "cBoKoKvQod@vQwQwQwj@oK_q@gpAwQ~Wod@vQwj@?wQ~WwQ?wQf^_XfEwQ~WwQfEwQgEwQnK_X?wQwQwj@fE_" \
                              "XoKoK?oKvQod@gEg^fEwQoKoK_XfE_q@f^_X~W_q@?wQ~W_Xnd@ovAoKg^od@od@?o}@nKwQfE_cBvQod@fEw" \
                              "cAoK_X?od@g^gbCg^wj@gEo}@g^wcA?_X_Xg^wQwcAwQ_Xf^ovA?wcAoKovA_XwQ?wQoKoKgE_q@nKg^oKoK_X" \
                              "gE_XfEwQoK?_q@_XovA?_XnKoKwQ_X?g^od@wQgEwQfEgEwQ_X?wj@nKgE?_XnKoKoKoKgEwj@wQoKgEod@od@" \
                              "od@oKgEwQfE_XwQnK_|BwQ_q@?wuBwQ_q@?_q@nKod@fEwuBnd@gpAgEooBnKwcAgEg^nK_q@?_jAnKwQvuB" \
                              "?fEoKf^?~p@vQfE_XnK?gE_XvQgw@fEgiB~p@_Xf^nKnKgE?oKfw@o}@oKwj@?_q@vQoKf^fEf^gpA?_XoKwQ" \
                              "?od@nKg^oKgEwQfE_X_XgE_q@nKoK?g^_X_q@fEod@wQoKg^_jAnK_`F_Xgw@fE_uCwj@gbCn}@giBnd@w|Afw" \
                              "@wcAvQgE~WvQ?g^vQwQfw@?fiBgiBnd@gE?_uCvj@oK~W_XvQ_q@f^oKnd@od@f^?vQf^?fbCfEfw@~W" \
                              "~WnKnzDvj@oK~p@f^?nvAod@vQvQ?nK~Wfw@gEnKnKfEvcAf^gEvQvQnKnd@?~p@~WfEvQo}@f^oKf^nd" \
                              "@?nKnK?nKf^vj@fEvQgEvQnKnKf^f^fEvQ~WfE~p@vj@~p@vQfpA~WvQnd@fEvQvQ?n}@nd@oKf^vQfEoKnd" \
                              "@fEnKwQvj@oKvQ_Xf^?nK~W?~p@nd@?nKwQf^?vQnK~WgE~WvQnK~Wnd@gE~p@~W~p@~p@oKn}@fEf^vQ?f" \
                              "^wQvj@nd@?nKvQvQ?fw@f^vj@vQfw@f^nKnd@gEvQoKnK_X~W?nK~WvQnKnKf^?vcAvQfpAg^n}@fEn" \
                              "}@wQnKwj@gE_Xf^vQnd@~iAvj@?~p@_jAnd@_X?oKnKwj@fEg^vQwj@fE?vQg^f^vQvQfEf^_Xf" \
                              "^_XnKgEvQnKf^gEnd@~WvQ?f^nd@gEnd@vj@vQn}@?vcAoKfEnK~Wnd@fEvQnd@nd@vQ~Wf^~p@noB?vQf^vQ" \
                              "?n}@oKvQwQfEgEvQg^nK?vQoKfE?~iAoKvQ?~Wod@~WoKgEwj@fEoKvQwQfE_XgEoKvQoK?fEnKoKnd@_XnK_q" \
                              "@v|A?~WvQvQfEf^fw@~p@?fw@g^~p@_X~WwQ?_q@_jAoKfE?nKgw@nKwQvQgEf^oKnKfEf^oKvj@_jA?fEn" \
                              "}@vQvQnKvj@~W~WnKnd@vj@~WfEvQf^vQn}@~iAnd@?vQnK~p@nvAnKvcAwQnd@oKfE_cBwQg^vQg" \
                              "^oKwQwQgE_XwQoK_XoKoKfEgw@oK_X_q@g^nK?vQ_XvQ?fw@_jAfpAwQfw@g^nd@?nd@~Wn}@?fw@_Xn" \
                              "}@oKfEgw@gEgEvj@nKf^oKf^_XvQ_Xvj@fEf^nKnK?fw@oKnd@_XnK?f^gEnKwQfEfEnd@oKnKgw@fEgE~W_q" \
                              "@??~p@wQf^?n}@oK~p@wQnKoKf^fE~p@oKvQoK?"

    greater_london_polygon = geometry.Polygon(polyline.decode(greater_london_polyline))

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            curs.execute(get_existing_london_areas_query)
            data = curs.fetchall()

    not_in_london = []

    for each in data:
        try:
            if not greater_london_polygon.contains(geometry.Polygon(polyline.decode(each[2]))):
                not_in_london.append(each[0])
        except IndexError:
            if not greater_london_polygon.contains(geometry.Polygon(polyline.decode(each[2] + '@'))):
                not_in_london.append(each[0])
        except TypeError:
            pass
        continue

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            curs.execute(update_area_status_query, (tuple(not_in_london),))


def write_webflow_london_nhoods(prioritised_only=True):
    if prioritised_only:
        nhoods_list = ["Acton",
                       "Aldgate",
                       "Angel",
                       "Balham",
                       "Barbican",
                       "Battersea",
                       "Bayswater",
                       "Belgravia",
                       "Bermondsey",
                       "Bethnal Green",
                       "Bexley",
                       "Bexleyheath",
                       "Blackfriars",
                       "Bow",
                       "Brixton",
                       "Bromley",
                       "Bromley Common",
                       "Camberwell",
                       "Camden Town",
                       "Canary Wharf",
                       "Canning Town",
                       "Chalk Farm",
                       "Charing Cross",
                       "Chelsea",
                       "Chinatown",
                       "Chiswick",
                       "Clapham",
                       "Clerkenwell",
                       "Covent Garden",
                       "Croydon",
                       "Dagenham",
                       "Dalston",
                       "Dulwich",
                       "Earls Court",
                       "Elephant and Castle",
                       "Farringdon",
                       "Finsbury Park",
                       "Fulham",
                       "Greenwich",
                       "Hackney",
                       "Hackney Central",
                       "Hackney Wick",
                       "Hammersmith",
                       "Highbury",
                       "Holborn",
                       "Holland Park",
                       "Hoxton",
                       "Islington",
                       "Kennington",
                       "Kensington",
                       "Kentish Town",
                       "Knightsbridge",
                       "Marylebone",
                       "Mayfair",
                       "Mile End",
                       "Newington",
                       "Notting Hill",
                       "Oval",
                       "Paddington",
                       "Parsons Green",
                       "Peckham",
                       "Pimlico",
                       "Putney",
                       "Shadwell",
                       "Shepherd's Bush",
                       "Shoreditch",
                       "Soho",
                       "South Kensington",
                       "Spitalfields",
                       "St Pancras",
                       "Stepney",
                       "Stockwell",
                       "Stoke Newington",
                       "Stratford",
                       "Streatham",
                       "Temple",
                       "Tooting",
                       "Tottenham",
                       "Tottenham Hale",
                       "Tower Hill",
                       "Vauxhall",
                       "Walthamstow",
                       "Wembley",
                       "Westminster",
                       "White City",
                       "Whitechapel",
                       "Wimbledon",
                       "Woolwich"]

        london_nhood_query = """
            SELECT nhood_id, nhood_name 
            FROM nhoods_uk
            WHERE in_london is TRUE AND nhood_name = ANY(%s)
        """

    else:
        london_nhood_query = """
            SELECT nhood_id, nhood_name 
            FROM nhoods_uk
            WHERE in_london is TRUE
            """

    url = "https://api.webflow.com/collections/{}/items".format(os.environ.get('WEBFLOW_COLLECTION_ID'))

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "Accept-Version": "1.0.0",
        "Content-Type": "application/json"
    }



    print("Getting all neighbourhoods in London from DB ...")
    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            if prioritised_only:
                curs.execute(london_nhood_query, (nhoods_list,))
            else:
                curs.execute(london_nhood_query,)
            data = curs.fetchall()

    for i, each in enumerate(data):
        payload = {
            "fields": {
                "_archived": False,
                "_draft": False,
                "name": str(each[0]),
                # required field by Webflow but doesn't accept spaces
                "slug": each[1].replace(' ', '_').replace("'", '').replace(',', ''),
                "nhood-name": each[1]
            }
        }

        try:
            print("Writing to Webflow CMS now ...")
            r = requests.post(url, headers=headers, json=payload)

            if r.status_code == 200:
                print("Finished writing neighbourhoods {} of {} to Webflow".format(i + 1, len(data)))

            else:
                print("An error occurred writing to Webflow: {}".format(r.content))
                print("Culprit object: {}".format(each))

            try:
                if int(r.headers['X-RateLimit-Remaining']) <= 1:  # 1 instead of 0 because of bug in Webflow API
                    print("Going to sleep for 70s to reset Webflow rate limit ...")
                    sleep(70)  # Sleep for 60s before making new requests to Webflow
            except KeyError:
                pass

        except Exception:
            print(traceback.format_exc())
            print("Culprit object: {}".format(each))
        continue


if __name__ == '__main__':
    timer_start = timeit.default_timer()
    steps = {
        Steps.UK_NHOODS_DB_POPULATE: False,
        Steps.CLEAN_DATA: False,
        Steps.WRITE_WEBFLOW: True,
        Steps.LONDON_CATS_NHOODS_DB_POPULATE: False
    }

    if steps[Steps.UK_NHOODS_DB_POPULATE]:
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
        standardised_nhoods = [standardise_nhoods_sql(x, [list(x.values())[0] for x in london_nhoods_wiki]) for x in
                               nhoods_rmv]
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

    if steps[Steps.CLEAN_DATA]:
        clean_data()

    if steps[Steps.WRITE_WEBFLOW]:
        write_webflow_london_nhoods()

    if steps[Steps.LONDON_CATS_NHOODS_DB_POPULATE]:
        print("Storing London neighbourhoods categorisations now ...")
        write_db_london_nhoods_cats("london_nhood_cats.csv")


    timer_end = timeit.default_timer()
    print("Finished and it took {} seconds".format(timer_end - timer_start))
