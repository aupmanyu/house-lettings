import requests


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