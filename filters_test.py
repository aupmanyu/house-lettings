import timeit
import psycopg2

import rmv_constants
import general_constants
import filters


def test_keyword_filtering():

    distinct_description_query = """
    SELECT distinct on (website_unique_id) description FROM property_listings;
    """
    keywords = [general_constants.CheckboxFeatures.GARDEN, general_constants.CheckboxFeatures.CONCIERGE,
                general_constants.CheckboxFeatures.PARKING_SPACE, general_constants.CheckboxFeatures.NO_GROUND_FLOOR]

    with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
        with conn.cursor() as curs:
            try:
                curs.execute(distinct_description_query)
                data = curs.fetchall()
                results = []
                for i, each in enumerate(data):
                    results.append({
                        'description': each[0]
                    })
                    for keyword in keywords:
                        results[i][keyword] = \
                            filters.keyword_filter(keyword, {rmv_constants.RmvPropDetails.description.name: each[0]})

                return results
            except Exception as e:
                print("An error occurred connecting to DB: {}".format(e))


start = timeit.default_timer()
filter_results = test_keyword_filtering()
end = timeit.default_timer()
print("It took {} seconds to run".format(end - start))
print(filter_results)