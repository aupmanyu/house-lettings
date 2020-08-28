import os
import math
import logging
import requests
from urllib3.util.retry import Retry

from util.requests_adapters import TimeoutHTTPAdapter

cms_logger = logging.getLogger("nectr." + __name__)

WEBFLOW_AUTH_TOKEN = os.getenv("WEBFLOW_AUTH_TOKEN")
WEBFLOW_BASE_URL = "https://api.webflow.com/collections"


def construct_url(cid):
    return WEBFLOW_BASE_URL + "/" + cid + "/items"


def assert_status_hook(response, *args, **kwargs):
    return response.raise_for_status()


retries = Retry(total=5, backoff_factor=8.75, status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["GET", "POST", "PATCH", "OPTIONS"])

adapter = TimeoutHTTPAdapter(timeout=3, max_retries=retries)

webflow_cms_session = requests.Session()
webflow_cms_session.mount("https://", adapter)
webflow_cms_session.mount("http://", adapter)
webflow_cms_session.hooks["response"] = [assert_status_hook]
webflow_cms_session.headers.update({"Authorization": "Bearer {}".format(WEBFLOW_AUTH_TOKEN)})
webflow_cms_session.headers.update({"Accept-Version": "1.0.0"})
webflow_cms_session.headers.update({"Content-Type": "application/json"})


def create_item(payload: dict, collection_id: str, live=True):
    url = construct_url(collection_id)
    if live:
        url = url + "?live=true"

    payload.update({
        "_archived": payload["_archived"] if "_archived" in payload else False,
        "_draft": payload["_draft"] if "_draft" in payload else False
    })

    payload = {"fields": payload}

    try:
        r = webflow_cms_session.post(url, json=payload)
        cms_logger.info("Sucessfully created item with ID {}".format(r.json()["_id"]))

    except requests.exceptions.HTTPError as errh:
        cms_logger.error(errh, exc_info=1)

    except requests.exceptions.ConnectionError as errc:
        cms_logger.error(errc, exc_info=1)

    except requests.exceptions.Timeout as errt:
        cms_logger.error(errt, exc_info=1)

    except requests.exceptions.RequestException as err:
        cms_logger.error(err, exc_info=1)


def get_items(collection_id: str):
    url = construct_url(collection_id)

    try:
        r = webflow_cms_session.get(url)
        data = r.json()
        limit = data["limit"]
        items = data["items"]
        iters = math.ceil(data["total"] / limit)

        for i in range(1, iters):
            r = webflow_cms_session.get(url, params={"offset": i * limit})
            items.extend(r.json()["items"])

        return items

    except requests.exceptions.HTTPError as errh:
        cms_logger.error(errh, exc_info=1)

    except requests.exceptions.ConnectionError as errc:
        cms_logger.error(errc, exc_info=1)

    except requests.exceptions.Timeout as errt:
        cms_logger.error(errt, exc_info=1)

    except requests.exceptions.RequestException as err:
        cms_logger.error(err, exc_info=1)


def patch_item(payload: dict, collection_id: str, item_id:str, live=True):
    url = construct_url(collection_id) + "/" + item_id

    payload.update({
        "_archived": payload["_archived"] if "_archived" in payload else False,
        "_draft": payload["_draft"] if "_draft" in payload else False
    })

    payload = {"fields": payload}

    try:
        r = webflow_cms_session.patch(url, json=payload)
        cms_logger.info("Sucessfully updated item with ID {}".format(item_id))

    except requests.exceptions.HTTPError as errh:
        cms_logger.error(errh, exc_info=1)

    except requests.exceptions.ConnectionError as errc:
        cms_logger.error(errc, exc_info=1)

    except requests.exceptions.Timeout as errt:
        cms_logger.error(errt, exc_info=1)

    except requests.exceptions.RequestException as err:
        cms_logger.error(err, exc_info=1)

