import uuid
import psycopg2
import polyline
import traceback
from shapely import geometry
from collections import namedtuple
from functools import reduce

from app import filters, general_constants, rmv_constants


class PropertyScorer:
    def __init__(self):
        self._db_url = general_constants.DB_URL
        self._db_conn = self._connect_db()
        self._london_areas_ids_names, self._london_polylines = self._get_london_areas_boundaries()
        self._london_cats = self._get_london_nhoods_cats()

    def __del__(self):
        self._db_conn.close()

    def _connect_db(self):
        return psycopg2.connect(self._db_url, sslmode='allow')

    def score(self, listing: dict, user_desired_areas: [str],
              user_desired_cats: [general_constants.NhoodCategorisation],
              user_keywords: [general_constants.CheckboxFeatures]):

        property_coords = self._extract_property_details(listing, 'coords')

        try:
            description = self._extract_property_details(listing, 'desc')
        except Exception as e:
            print(e)
            description = ''

        Score_Weight = namedtuple('Score_Weight', ['area', 'cat', 'keyword'])
        weights = Score_Weight(10, 0.5, 1)
        try:
            result_areas = sum([int(x) for x in self._in_user_desired_areas(property_coords, user_desired_areas)
                                if x is not None])
            result_cats = sum([int(x) for x in self._in_user_desired_categories(property_coords, user_desired_cats)
                               if x is not None])
            result_keywords = sum([int(self._matches_keyword(x, description)) for x in user_keywords])

            results = Score_Weight(result_areas, result_cats, result_keywords)

        except Exception:
            traceback.format_exc()
            print("CULPRIT: {}".format(listing))
            return 0

        return sum([a*b for a, b in zip(weights, results)])

    def _in_user_desired_areas(self, property_coords: tuple, user_desired_areas: [str]):
        in_london = reduce(lambda x, y: x or y, [True for x in user_desired_areas
                                                 if x in self._london_areas_ids_names.values()], 0)
        london_polylines_lookup = {x.nhood_name: x for x in self._london_polylines}
        Point_XY = geometry.Point(property_coords)
        if in_london:
            target_areas = [self._decode_polyline(london_polylines_lookup[x].polyline)
                            if london_polylines_lookup[x].polyline is not None else None for x in user_desired_areas ]

            return [geometry.Polygon(polygon).contains(Point_XY) if polygon is not None else None
                    for polygon in target_areas]
        else:
            return []

    def _in_user_desired_categories(self, property_coords: tuple,
                                    user_desired_cats: [general_constants.NhoodCategorisation]):
        target_nhoods = [self._london_areas_ids_names[each.nhood_id] for cat in user_desired_cats
                         for each in self._london_cats if getattr(each, cat.name) > 0]
        # for each in self._london_cats:
        #     for cat in user_desired_cats:
        #         if getattr(each, cat.name) > 0:
        #             target_nhoods.append(each.nhood_id)

        # target_nhoods = [self._london_areas_ids_names[x] for x in target_nhoods]

        return self._in_user_desired_areas(property_coords, target_nhoods)

    @staticmethod
    def _matches_keyword(keyword: general_constants.CheckboxFeatures, description: str):
        return filters.keyword_filter(keyword, description)

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
        # print("Decoding polyline: {}".format(enc_polyline))
        return polyline.decode(enc_polyline)

    @staticmethod
    def _extract_property_details(listing: dict, detail=None):
        try:
            if detail == 'coords':
                return (float(listing[rmv_constants.RmvPropDetails.geo_lat.name]),
                        float(listing[rmv_constants.RmvPropDetails.geo_long.name]))
            elif detail == 'desc':
                return listing[rmv_constants.RmvPropDetails.description.name]
            else:
                raise ValueError("You need to provide a detail to extract. Possible values: coords, desc")
        except Exception as e:
            print("An error occurred filtering property: {}. CULPRIT: {} ".format(e, listing))


if __name__ == '__main__':
    ranker = PropertyScorer()

    # in Islington (artsy), no ground floor keyword match - expected score 11.5
    listing_1 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.544743,
        rmv_constants.RmvPropDetails.geo_long.name: -0.102888,
        rmv_constants.RmvPropDetails.description.name: "Letting information:        Furnishing: Furnished   Letting "
                                                       "type: Long term   Added on Rightmove:  06 March 2020 (13 days "
                                                       "ago)      Key features  Fitted kitchen Furnished 3 minutes "
                                                       "walk to Farringdon Station 3 minutes walk to Chancery Lane "
                                                       "Station   Full description          Spacious top floor 1 "
                                                       "bedroom apartment located on Hatton Garden, furnished, "
                                                       "modern fully fitted kitchen, plenty of natural light, "
                                                       "3 minutes walk in either direction to Farringdon or Chancery "
                                                       "Lane tube Stations EPC Rating D "
    }

    # in Soho (artsy), garden, parking, no ground floor keyword match - expected score 12.5
    listing_2 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.512548,
        rmv_constants.RmvPropDetails.geo_long.name: -0.133226,
        rmv_constants.RmvPropDetails.description.name: "Letting information:        Furnishing: Unfurnished   Added "
                                                       "on Rightmove:  08 January 2020 (71 days ago)      Full "
                                                       "description          The property comprises of a spacious "
                                                       "lounge, Fitted Kitchen/ Dining area, Kitchen comes with "
                                                       "Washing Machine and integrated Dishwasher, Additional Ground "
                                                       "Floor W.C, Double doors opening to rear garden, Three Double "
                                                       "bedrooms, Part tiled fitted bathroom with shower attachment, "
                                                       "Neutral decor, Fitted carpet, Wood Flooring, Gas central "
                                                       "heating, Double Glazing, Rear Garden and Off Street Parking "
                                                       "big enough for two cars. "
    }

    # in Bermondsey (artsy), parking space, no ground floor keyword match - expected score 2.5
    listing_3 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.491746,
        rmv_constants.RmvPropDetails.geo_long.name: -0.065278,
        rmv_constants.RmvPropDetails.description.name: "Letting information:        Date available: 08/05/2020   "
                                                       "Furnishing: Furnished   Added on Rightmove:  11 March 2020 (8 "
                                                       "days ago)      Key features  Private Riverside Development "
                                                       "Balcony Off Street Parking En-Suite Security Entryphone "
                                                       "System   Full description          Property comprises south "
                                                       "facing reception, 2 double bedrooms, 2 private balconies, "
                                                       "appliance fitted kitchen, en-suite shower room and main "
                                                       "bathroom. Additional benefits include security entry phone "
                                                       "system and off street parking. A short walk over the bridge "
                                                       "brings you close to Waitrose, Costa and Greenwich market "
                                                       "along with all the numerous dine out options. With Cutty Sark "
                                                       "DLR and Greenwich Train station less than 10mins walk away, "
                                                       "connections into the City, Canary Wharf and Stratford are all "
                                                       "within easy reach. Available furnished from 8th May 2020 so "
                                                       "could this be Ur perfect new home? More photos to be added. "
    }

    # in Battersea (green, artsy), garden, no ground floor keyword match - expected score 3
    listing_4 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.469138,
        rmv_constants.RmvPropDetails.geo_long.name: -0.155479,
        rmv_constants.RmvPropDetails.description.name: "Letting information:        Date available: Now   Furnishing: "
                                                       "Unfurnished   Reduced on Rightmove:  13 March 2020 (6 days "
                                                       "ago)      Key features  Two Double Bedrooms Large Open Plan "
                                                       "Reception High Ceilings Throughout High Specification Private "
                                                       "Balcony Recently Refurbished Offered Unfurnished Moments from "
                                                       "Finsbury Park Tube   Full description          A stylish two "
                                                       "double bedroom apartment within this charming period "
                                                       "Victorian house in a prime Finsbury location.Spacious double "
                                                       "aspect reception with high ceilings, large window to the "
                                                       "front of the property and Juliette balcony to the rear "
                                                       "overlooking the garden in the kitchen. This beautifully "
                                                       "well-presented property has been refurbished to the highest "
                                                       "level, with high spec specification throughout.At one end of "
                                                       "this spacious reception is a well appointed kitchen, "
                                                       "with ample storage space, built in appliances and gas hob. "
                                                       "The stone counter tops complement the sleek design "
                                                       "throughout. Two double bedrooms, with the master offering "
                                                       "access to a private rear roof terrace which is decked.There "
                                                       "is wood flooring throughout giving a clean crisp feel. Entry "
                                                       "video phone system with CCTV throughout the building which "
                                                       "can be accessed remotely for great security.Located within "
                                                       "moments of Finsbury Park which offers connections on the "
                                                       "Piccadilly line, for speedy access into Central London and "
                                                       "beyond.A truly exceptional property, which is one of the best "
                                                       "examples you will find in the neighbouring area.    More "
                                                       "information from this agent To view this media, please visit "
                                                       "the on-line version of this page at "
                                                       "www.rightmove.co.uk/property-to-rent/property-52780242.html"
                                                       "?premiumA=true  Particulars "
    }

    # in Aldgate, concierge, no ground floor key word match - expected score 2
    listing_5 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.512110,
        rmv_constants.RmvPropDetails.geo_long.name: -0.070850,
        rmv_constants.RmvPropDetails.description.name: "Letting information:        Date available: Now   Furnishing: "
                                                       "Furnished   Added on Rightmove:  02 March 2020 (17 days ago)  "
                                                       "    Full description          Felicity J Lord are excited to "
                                                       "present this fabulous studio apartment in the prestigious Pan "
                                                       "Peninsula development. Finished to a high spec this studio "
                                                       "apartment has a white finish boasting a spacious a living "
                                                       "area which leads out to private balcony with fabulous views "
                                                       "over London from the 14th Floor. Benefits include unrivalled "
                                                       "leisure facilities with a gym, pool, cinema and spa all "
                                                       "onsite and a 24hour concierge.    More information from this "
                                                       "agent To view this media, please visit the on-line version of "
                                                       "this page at "
                                                       "www.rightmove.co.uk/property-to-rent/property-27486609.html"
                                                       "?premiumA=true  Particulars   Energy Performance Certificates "
                                                       "(EPCs) To view this media, please visit the on-line version "
                                                       "of this page at "
                                                       "www.rightmove.co.uk/property-to-rent/property-27486609.html"
                                                       "?premiumA=true  Download EPC for this property "
    }

    # in North Finchley, no key word match - expected score 0
    listing_6 = {
        rmv_constants.RmvPropDetails.geo_lat.name: 51.613810,
        rmv_constants.RmvPropDetails.geo_long.name: -0.174851,
        rmv_constants.RmvPropDetails.description.name: "Some description ground floor"
    }

    # property_coords_1 = (51.544743, -0.102888)  # in Islington
    # property_coords_2 = (51.512548, -0.133226)  # in Soho
    # property_coords_3 = (51.491746, -0.065278)  # in Bermondsey
    # property_coords_4 = (51.469138, -0.155479)  # in Battersea ("green" neighbourhood)
    # property_coords_5 = (51.512110, -0.070850)  # in Aldgate (not "green" neighbourhood)
    # user_desired_areas = ['Soho', 'Islington', 'Angel', 'Hackney']
    # user_desired_cats = [general_constants.NhoodCategorisation.green, general_constants.NhoodCategorisation.artsy]
    # user_keywords = [general_constants.CheckboxFeatures.GARDEN, general_constants.CheckboxFeatures.CONCIERGE,
    #                  general_constants.CheckboxFeatures.PARKING_SPACE,
    #                  general_constants.CheckboxFeatures.NO_GROUND_FLOOR]

    user_desired_areas = user_desired_cats = user_keywords = []

    result_1 = ranker.score(listing_1, user_desired_areas, user_desired_cats, user_keywords)
    result_2 = ranker.score(listing_2, user_desired_areas, user_desired_cats, user_keywords)
    result_3 = ranker.score(listing_3, user_desired_areas, user_desired_cats, user_keywords)
    result_4 = ranker.score(listing_4, user_desired_areas, user_desired_cats, user_keywords)
    result_5 = ranker.score(listing_5, user_desired_areas, user_desired_cats, user_keywords)
    result_6 = ranker.score(listing_6, user_desired_areas, user_desired_cats, user_keywords)

    print("Finished")