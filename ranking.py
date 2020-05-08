from __future__ import annotations

from sys import stderr
import uuid
import psycopg2
import polyline
import traceback

import requests.exceptions
from shapely import geometry
from collections import namedtuple
from functools import reduce

import filters
import rmv_constants
import general_constants
from image_annotation import EveryPixelAnnotator, GoogleVisionAnnotator, BillingLimitException


class PropertyScorer:
    def __init__(self):
        self._db_url = general_constants.DB_URL
        self._db_conn = self._connect_db()
        self._london_areas_ids_names, self._london_polylines = self._get_london_areas_boundaries()
        self._london_cats = self._get_london_nhoods_cats()
        self._image_annotator = ImageAnnotatorRouter()

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
            image_urls = self._extract_property_details(listing, 'images')
        except Exception as e:
            print(e)
            description = ''
            image_urls = []

        Score_Vector = namedtuple('Score_Vector', ['area', 'cat', 'keyword'])
        weights = Score_Vector(10, 0.5, 1)
        try:
            result_areas = sum([int(x) for x in self._in_user_desired_areas(property_coords, user_desired_areas)
                                if x is not None])
            result_cats = sum([int(x) for x in self._in_user_desired_categories(property_coords, user_desired_cats)
                               if x is not None])

            result_keywords = []
            for keyword in user_keywords:
                try:
                    result_keywords.append(int(self._matches_keyword(keyword, description, image_urls)))
                except (ValueError, FileNotFoundError, BillingLimitException, requests.exceptions.HTTPError):
                    print(traceback.format_exc(), file=stderr)
                    pass
                continue

            result_keywords = sum(result_keywords)
            results = Score_Vector(result_areas, result_cats, result_keywords)

        except Exception:
            print(traceback.format_exc(), file=stderr)
            print("CULPRIT: {}".format(listing))
            return 0

        return sum([a * b for a, b in zip(weights, results)])

    def _in_user_desired_areas(self, property_coords: tuple, user_desired_areas: [str]) -> [bool]:
        in_london = reduce(lambda x, y: x or y, [True for x in user_desired_areas
                                                 if x in self._london_areas_ids_names.values()], 0)
        london_polylines_lookup = {x.nhood_name: x for x in self._london_polylines}
        Point_XY = geometry.Point(property_coords)
        if in_london:
            target_areas = [self._decode_polyline(london_polylines_lookup[x].polyline)
                            if london_polylines_lookup[x].polyline is not None else None for x in user_desired_areas]

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

    def _matches_keyword(self, keyword: general_constants.CheckboxFeatures, description: str, image_urls: [str]) -> bool:
        try:
            if keyword in [general_constants.CheckboxFeatures.PARKING_SPACE,
                           general_constants.CheckboxFeatures.NO_GROUND_FLOOR,
                           general_constants.CheckboxFeatures.GARDEN,
                           general_constants.CheckboxFeatures.CONCIERGE]:
                return self._matches_keyword_text(keyword, description)
            elif keyword in [general_constants.CheckboxFeatures.MODERN_INTERIORS,
                             general_constants.CheckboxFeatures.WOODEN_FLOORS,
                             general_constants.CheckboxFeatures.BRIGHT,
                             general_constants.CheckboxFeatures.OPEN_PLAN_KITCHEN]:
                return self._matches_keyword_vision(keyword, image_urls)
            else:
                raise ValueError("{msg: Keyword {} not supported".format(keyword))
        except (ValueError, FileNotFoundError, requests.exceptions.HTTPError) as e:
            raise e

    @staticmethod
    def _matches_keyword_text(keyword: general_constants.CheckboxFeatures, description: str) -> bool:
        return filters.keyword_filter(keyword, description)

    def _matches_keyword_vision(self, keyword: general_constants.CheckboxFeatures, image_urls: [str]) -> bool:
        decision = []
        for image_url in image_urls:
            try:
                annotations = self._image_annotator.route(image_url, keyword)
                if "modern" in annotations:
                    decision.append(True)
            except (ValueError, FileNotFoundError, requests.exceptions.HTTPError) as e:
                raise e

        if decision.count(True) >= 2:
            return True
        else:
            return False

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
            elif detail == 'images':
                return listing[rmv_constants.RmvPropDetails.image_links.name]
            else:
                raise ValueError("You need to provide a detail to extract. Possible values: coords, desc, images")
        except Exception as e:
            print("An error occurred extracting property field: {}. CULPRIT: {} ".format(e, listing))


class ImageAnnotatorRouter(object):
    def __init__(self):
        self._ep_annotator = EveryPixelAnnotator()
        self._gvision_annotator = GoogleVisionAnnotator()

    def route(self, image_url: str, keyword: general_constants.CheckboxFeatures) -> dict:
        if keyword is general_constants.CheckboxFeatures.MODERN_INTERIORS:
            return self._ep_annotator.annotate(image_url)
        elif keyword is general_constants.CheckboxFeatures.WOODEN_FLOORS:
            return self._gvision_annotator.annotate(image_url)
        else:
            raise ValueError("Keyword {} not currently supported by image annotators".format(keyword))


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
                                                       "Lane tube Stations EPC Rating D ",
        rmv_constants.RmvPropDetails.image_links.name: ["https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_01_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_02_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_03_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_04_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_05_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/33k/32903/78125053/32903_1789749_IMG_06_0000_max_656x437.jpg"]
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
                                                       "big enough for two cars. ",
        rmv_constants.RmvPropDetails.image_links.name: ["https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_04_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_08_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_09_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_10_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_11_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_12_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_13_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_14_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_15_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_16_0000_max_656x437.JPG",
                                                        "https://media.rightmove.co.uk/dir/56k/55234/21726857/55234_USRE3_IMG_17_0000_max_656x437.JPG"]
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
                                                       "could this be Ur perfect new home? More photos to be added. ",
        rmv_constants.RmvPropDetails.image_links.name: ["https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_01_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_10_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_02_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_11_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_09_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_05_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_06_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_07_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_08_0000_max_656x437.jpg",
                                                        "https://media.rightmove.co.uk/dir/97k/96668/78049624/96668_74183708032020_IMG_12_0000_max_656x437.jpg"]
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
    user_desired_areas = ['Soho', 'Islington', 'Angel', 'Hackney']
    user_desired_cats = [general_constants.NhoodCategorisation.green, general_constants.NhoodCategorisation.artsy]
    user_keywords = [general_constants.CheckboxFeatures.WOODEN_FLOORS,
                     general_constants.CheckboxFeatures.MODERN_INTERIORS,
                     general_constants.CheckboxFeatures.PARKING_SPACE,
                     general_constants.CheckboxFeatures.NO_GROUND_FLOOR]

    # user_desired_areas = user_desired_cats = user_keywords = []

    result_1 = ranker.score(listing_1, user_desired_areas, user_desired_cats, user_keywords)
    result_1b = ranker.score(listing_1, user_desired_areas, user_desired_cats, user_keywords)  # to test cache
    result_2 = ranker.score(listing_2, user_desired_areas, user_desired_cats, user_keywords)
    result_3 = ranker.score(listing_3, user_desired_areas, user_desired_cats, user_keywords)
    result_4 = ranker.score(listing_4, user_desired_areas, user_desired_cats, user_keywords)
    result_5 = ranker.score(listing_5, user_desired_areas, user_desired_cats, user_keywords)
    result_6 = ranker.score(listing_6, user_desired_areas, user_desired_cats, user_keywords)

    print("Finished")
