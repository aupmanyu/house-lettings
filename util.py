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


def find_value_nested_dict(input_dict: dict, value: str):
    for k, v in input_dict.items():
        if isinstance(v, dict):
            find_value_nested_dict(v, value)
        elif isinstance(v, list):
            [find_value_nested_dict(item, value) for item in v]
        else:
            if k == "@type" and v == value:
                print(input_dict)
                break
