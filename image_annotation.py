import os
import requests

from functools import lru_cache
from google.cloud import vision
from abc import ABC, abstractmethod
from google.cloud.vision_v1 import types


class IVisionAnnotator(ABC):
    @abstractmethod
    def annotate(self, image_url: str) -> dict:
        pass


class EveryPixelAnnotator(IVisionAnnotator):
    def __init__(self):
        self._client_id = os.environ.get("EP_CLIENT_ID")
        self._client_secret = os.environ.get("EP_CLIENT_SECRET")
        self._base_url = "https://api.everypixel.com/v1"
        self._request_count = 0
        self._max_requests = 0.8 * 100  # 80% of free limit in case we ran some requests elsewhere for testing

    @lru_cache(maxsize=128)
    def annotate(self, image_url: str):
        ep_url = self._base_url + "/keywords"
        params = {
            "url": image_url,
            "threshold": 0.75
        }

        if self._request_count <= self._max_requests:
            try:
                r = requests.get(ep_url, params=params, auth=(self._client_id, self._client_secret))
                r.raise_for_status()
                self._request_count += 1
                data = r.json()
                keywords = {}
                for each in data['keywords']:
                    if each["keyword"] == "Modern":
                        keywords["modern"] = each["score"]
                    elif each["keyword"] == "Luxury":
                        keywords["luxury"] = each["score"]
                    elif each["keyword"] == "Elegance":
                        keywords["elegance"] = each["score"]
                    elif each["keyword"] == "Indoors" or each["keyword"] == "Domestic Room":
                        if "indoors" in keywords:
                            # take simple average of "indoors" & "domestic room" score to determine if indoors
                            keywords["indoors"] = (keywords["indoors"] + each["score"]) / 2
                        else:
                            keywords["indoors"] = each["score"]

                return keywords

            except KeyError:
                raise FileNotFoundError("msg: {}".format(data)) from None

            except requests.exceptions.HTTPError as e:
                raise e

        else:
            raise BillingLimitException("{msg: Hit configured limit of API. {} of {} requests made}"
                                        .format(self._request_count, self._max_requests))


class GoogleVisionAnnotator(IVisionAnnotator):
    def __init__(self):
        self._client = vision.ImageAnnotatorClient()
        self._threshold = 0.75
        self._request_count = 0
        self._max_requests = 0.8 * 1000  # NB: 1000 is in units (https://cloud.google.com/vision/pricing)

    @lru_cache(maxsize=1024)
    def annotate(self, image_url: str):
        if self._request_count < self._max_requests:
            image = types.Image()
            image.source.image_uri = image_url
            response = self._client.label_detection(image=image, max_results=10)
            self._request_count += 1
            labels = response.label_annotations
            keywords = {}
            for label in labels:
                if label.score >= self._threshold:
                    if label.description == "Wood flooring" or label.description == "Laminate flooring" \
                            or label.description == "Hardwood":
                        if "wooden_floors" in keywords:
                            (keywords["wooden_floors"]).append(label.score)
                        else:
                            keywords["wooden_floors"] = [label.score]
            try:
                keywords["wooden_floors"] = sum(keywords["wooden_floors"]) / len(keywords["wooden_floors"])
            except KeyError:
                pass

            return keywords

        else:
            raise BillingLimitException("{msg: Hit configured limit of API. {} of {} requests made}"
                                        .format(self._request_count, self._max_requests))


class BillingLimitException(Exception):
    '''Raise when we are at risk of going over our billing limit'''