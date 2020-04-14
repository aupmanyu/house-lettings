import uuid
import psycopg2
import polyline
from shapely import geometry
from collections import namedtuple
from functools import reduce

import rmv_constants
import general_constants


class PropertyRanker:
    def __init__(self):
        self._db_url = general_constants.DB_URL
        self._db_conn = self._connect_db()
        self._london_areas_ids_names, self._london_polylines = self._get_london_areas_boundaries()
        self._london_cats = self._get_london_nhoods_cats()

    def __del__(self):
        self._db_conn.close()

    def _connect_db(self):
        return psycopg2.connect(self._db_url, sslmode='allow')

    def in_user_desired_areas(self, property_coords: tuple, user_desired_areas: [str]):
        in_london = reduce(lambda x, y: x or y, [True for x in user_desired_areas
                                                 if x in self._london_areas_ids_names.values()])
        london_polylines_lookup = {x.nhood_name: x for x in self._london_polylines}
        Point_XY = geometry.Point(property_coords)
        if in_london:
            target_areas = [self._decode_polyline(london_polylines_lookup[x].polyline)
                                  for x in user_desired_areas if london_polylines_lookup[x].polyline is not None]

        return reduce(lambda x, y: x or y, [geometry.Polygon(polygon).contains(Point_XY) for polygon in target_areas])

    def in_user_desired_categories(self, property_coords: tuple, user_desired_cats: [general_constants.NhoodCategorisation]):
        target_nhoods = [self._london_areas_ids_names[each.nhood_id] for cat in user_desired_cats
                         for each in self._london_cats if getattr(each, cat.name) > 0]
        # for each in self._london_cats:
        #     for cat in user_desired_cats:
        #         if getattr(each, cat.name) > 0:
        #             target_nhoods.append(each.nhood_id)

        # target_nhoods = [self._london_areas_ids_names[x] for x in target_nhoods]

        return self.in_user_desired_areas(property_coords, target_nhoods)

    def matches_keywords(self, user_keywords: list):
        pass

    def _get_areas_boundaries(self, areas_ids: [uuid]):
        get_polyline_query = """
        SELECT nhoods_uk.nhood_id, nhoods_uk.nhood_name, nhoods_uk.polyline 
        FROM nhoods_uk
        WHERE nhood_id in %s;       
        """

        Boundary_Result = namedtuple("Boundary_Result", ["nhood_id", "nhood_name", "polyline"])

        with self._db_conn as conn:
            with conn.cursor() as curs:
                # print(curs.mogrify(get_polyline_query, (tuple(areas_ids),)))
                curs.execute(get_polyline_query, (tuple(areas_ids),))
                data = [Boundary_Result(*x) for x in curs.fetchall()]

        return data

    def _get_london_areas_boundaries(self):
        get_london_areas_query = """
        SELECT DISTINCT ON (nhood_name) nhood_id, nhood_name
        FROM nhoods_uk
        WHERE in_london is TRUE
        """

        with self._db_conn as conn:
            with conn.cursor() as curs:
                curs.execute(get_london_areas_query, )
                data = curs.fetchall()
                areas_names_ids = {x[0]: x[1] for x in data}
                areas_ids = list(areas_names_ids.keys())

        london_polylines = self._get_areas_boundaries(areas_ids)
        return areas_names_ids, london_polylines

    def _get_london_nhoods_cats(self):
        get_london_nhoods_cats_query = """
        SELECT * FROM nhoods_cat
        """

        field_names = ['nhood_id'] + [x.name for x in general_constants.NhoodCategorisation]

        Cat_Result = namedtuple('Cat_Result', field_names)

        with self._db_conn as conn:
            with conn.cursor() as curs:
                curs.execute(get_london_nhoods_cats_query, )
                data = [Cat_Result(*x) for x in curs.fetchall()]

        return data

    @staticmethod
    def _decode_polyline(enc_polyline: str):
        print("Decoding polyline: {}".format(enc_polyline))
        return polyline.decode(enc_polyline)

    @staticmethod
    def _extract_property_coords(listing: dict):
        return (listing[rmv_constants.RmvPropDetails.geo_lat.name], listing[rmv_constants.RmvPropDetails.geo_long.name])


if __name__ == '__main__':
    ranker = PropertyRanker()
    property_coords_1 = (51.544743, -0.102888)  # in Islington
    property_coords_2 = (51.512548, -0.133226)  # in Soho
    property_coords_3 = (51.491746, -0.065278)  # in Bermondsey
    property_coords_4 = (51.469138, -0.155479)  # in Battersea ("green" neighbourhood)
    property_coords_5 = (51.512110, -0.070850)  # in Aldgate (not "green" neighbourhood)
    user_desired_areas = ['Soho', 'Islington', 'Angel', 'Hackney']
    user_desired_cats = [general_constants.NhoodCategorisation.green]

    result_4 = ranker.in_user_desired_categories(property_coords_4, user_desired_cats)
    result_5 = ranker.in_user_desired_categories(property_coords_5, user_desired_cats)

    result_1 = ranker.in_user_desired_areas(property_coords_1, user_desired_areas)
    result_2 = ranker.in_user_desired_areas(property_coords_2, user_desired_areas)
    result_3 = ranker.in_user_desired_areas(property_coords_3, user_desired_areas)


    print("Result 1: {}, Result 2: {}, Result 3: {}".format(result_1, result_2, result_3))
