import os
from time import sleep
import requests
from enum import Enum
from collections import namedtuple

from general_constants import Coords

TFL_BASE_URL = "https://api.tfl.gov.uk"

TubeLine = namedtuple("TubeLine", ["common_name", "colour"])


class TubeLines(Enum):
    BAKERLOO = TubeLine("Bakerloo", "#b26300")
    CENTRAL = TubeLine("Central", "#dc241f")
    CIRCLE = TubeLine("Circle", "#ffd329")
    DISTRICT = TubeLine("District", "#007d32")
    HAMMERSMITH_CITY = TubeLine("Hammersmith & City", "#f4a9be")
    JUBILEE = TubeLine("Jubilee", "#a1a5a7")
    METROPOLITAN = TubeLine("Metropolitan","#9b0058")
    NORTHERN = TubeLine("Northern", "#000000")
    PICCADILLY = TubeLine("Piccadilly", "#0019a8")
    VICTORIA = TubeLine("Victoria", "#0098d8")
    WATERLOO_CITY = TubeLine("Waterloo & City", "#93ceba")


def get_all_tube_stops_lines():
    url = TFL_BASE_URL + "/StopPoint/Mode/tube"

    print("Getting tube stop info from TfL")
    r = requests.get(url)

    if r.status_code == 200:
        data = r.json()
        stop_type = "NaptanMetroStation"

        #  API returns a lot of different things so we first need to extract tube stop data
        tube_stop_data = [x for x in data["stopPoints"] if x["stopType"] == stop_type]

        tube_stops = {}
        for each in tube_stop_data:
            station_name = each["commonName"]
            station_id = each["id"]
            coords = Coords(each["lat"], each["lon"])

            for x in each["additionalProperties"]:
                if x["key"] == "Zone":
                    station_zone = list(map(int, x["value"].replace("/", "+").split("+")))

            for mode_group in each["lineModeGroups"]:
                if mode_group["modeName"] == "tube":
                    lines = mode_group["lineIdentifier"]

            tube_stops[station_name] = {
                "station_id": station_id,
                "lines": lines,
                "station_zone": station_zone,
                "coords": coords
            }

        return tube_stops


def write_webflow_cms(tube_stops: dict):
    url = "https://api.webflow.com/collections/5eaf0803a0d3e484ca69b0db/items"

    headers = {
        "Authorization": "Bearer {}".format(os.environ['WEBFLOW_API_KEY']),
        "Accept-Version": "1.0.0",
        "Content-Type": "application/json"
    }

    for i, (k, v) in enumerate(tube_stops.items()):
        payload = {
            "fields": {
                "_archived": False,
                "_draft": False,
                "name": k,
                "slug": v["station_id"],
                "zone": " & ".join(map(str, v["station_zone"]))
            }
        }

        for j, line in enumerate(v["lines"]):
            payload["fields"]["tube-line-{}".format(j+1)] = TubeLines[line.upper().replace("-", "_")].value.common_name
            payload["fields"]["line-colour-{}".format(j+1)] = TubeLines[line.upper().replace("-", "_")].value.colour

        r = requests.post(url, headers=headers, json=payload)

        if r.status_code == 200:
            print("Finished writing tube stop {} of {} to Webflow".format(i + 1, len(tube_stops)))
            print("{} more requests remaining before hitting limit".format(int(r.headers['X-RateLimit-Remaining'])))

        else:
            print("An error occurred writing to Webflow: {}".format(r.content))
            print("CULPRIT: {}".format(payload))

        try:
            if int(r.headers['X-RateLimit-Remaining']) <= 1:  # 1 instead of 0 because of bug in Webflow API
                print("Going to sleep for 70s to reset Webflow rate limit ...")
                sleep(70)  # Sleep for 60s before making new requests to Webflow
        except KeyError:
            pass


# def verify_js_stop_names(tube_stops: dict):
#     js_names = [
#                    "Abbey Road",
#                     "West Acton",
#                     "Acton Town",
#                     "Aldgate",
#                     "Aldgate East",
#                     "Alperton",
#                     "Amersham",
#                     "Angel",
#                     "Archway",
#                     "Arnos Grove",
#                     "Arsenal",
#                     "Baker Street",
#                     "Balham" ,
#                     "Bank",
#                     "Barbican",
#                     "Barking",
#                     "Barkingside",
#                     "Barons Court",
#                     "Bayswater",
#                     "Becontree",
#                     "Belsize Park" ,
#                     "Bermondsey",
#                     "Bethnal Green",
#                     "Blackfriars",
#                     "Blackhorse Road",
#                     "Bond Street",
#                     "Borough" ,
#                     "Boston Manor",
#                     "Bounds Green",
#                     "Bow Road",
#                     "Brent Cross",
#                     "Brixton",
#                     "Bromley-By-Bow",
#                     "Buckhurst Hill",
#                     "Burnt Oak",
#                     "Caledonian Road",
#                     "Camden Town",
#                     "Canada Water",
#                     "Canary Wharf",
#                     "Canning Town",
#                     "Cannon Street" ,
#                     "Canons Park" ,
#                     "Chalfont and Latimer",
#                     "Chalk Farm" ,
#                     "Chancery Lane",
#                     "Charing Cross",
#                     "Chesham",
#                     "Chigwell",
#                     "Chiswick Park",
#                     "Chorleywood",
#                     "Clapham Common",
#                     "Clapham North" ,
#                     "Clapham South" ,
#                     "Cockfosters" ,
#                     "Colindale" ,
#                     "Colliers Wood",
#                     "Covent Garden" ,
#                     "Croxley",
#                     "Dagenham East",
#                     "Dagenham Heathway",
#                     "Debden",
#                     "Dollis Hill",
#                     "Ealing Broadway",
#                     "Ealing Common",
#                     "Earls Court",
#                     "East Acton",
#                     "Eastcote",
#                     "East Finchley",
#                     "East Ham",
#                     "East Putney",
#                     "Edgware",
#                     "Edgware Road",
#                     "Elephant and Castle",
#                     "Elm Park",
#                     "Embankment",
#                     "Epping",
#                     "Euston",
#                     "Euston Square",
#                     "Fairlop",
#                     "Farringdon" ,
#                     "Finchley Central",
#                     "Finchley Road" ,
#                     "Finsbury Park",
#                     "Fulham Broadway" ,
#                     "Imperial Wharf",
#                     "Gants Hill",
#                     "Gloucester Road",
#                     "Golders Green",
#                     "Goldhawk Road" ,
#                     "Goodge Street",
#                     "Grange Hill",
#                     "Great Portland Street",
#                     "Green Park" ,
#                     "Greenford",
#                     "Gunnersbury",
#                     "Hainault",
#                     "Hammersmith",
#                     "Hampstead",
#                     "Hanger Lane",
#                     "Harlesden" ,
#                     "Harrow and Wealdstone",
#                     "Harrow on the Hill",
#                     "Hatton Cross" ,
#                     "Heathrow Airport",
#                     "Hendon Central",
#                     "High Barnet",
#                     "Highbury and Islington",
#                     "Highgate",
#                     "High Street Kensington",
#                     "Hillingdon",
#                     "Holborn",
#                     "Holland Park",
#                     "Holloway Road" ,
#                     "Hornchurch",
#                     "Hounslow Central",
#                     "Hounslow East" ,
#                     "Hounslow West" ,
#                     "Hyde Park Corner",
#                     "Ickenham",
#                     "Kennington" ,
#                     "Kensal Green",
#                     "Kentish Town" ,
#                     "Kenton",
#                     "Kew Gardens",
#                     "Kilburn",
#                     "Kilburn Park",
#                     "Kings Cross",
#                     "St Pancras",
#                     "Kingsbury",
#                     "Knightsbridge",
#                     "Ladbroke Grove",
#                     "Lambeth North",
#                     "Lancaster Gate",
#                     "Latimer Road",
#                     "Leicester Square",
#                     "Leyton",
#                     "Leytonstone",
#                     "Liverpool Street",
#                     "London Bridge",
#                     "Loughton",
#                     "Maida Vale",
#                     "Manor House",
#                     "Mansion House",
#                     "Marble Arch",
#                     "Marylebone",
#                     "Mile End",
#                     "Mill Hill East",
#                     "Monument",
#                     "Moorgate",
#                     "Moor Park",
#                     "Morden",
#                     "Mornington Crescent",
#                     "Neasden",
#                     "Newbury Park",
#                     "New Cross",
#                     "New Cross Gate",
#                     "North Acton",
#                     "North Ealing",
#                     "Northfields",
#                     "North Greenwich",
#                     "North Harrow",
#                     "Northolt",
#                     "North Wembley",
#                     "Northwick Park",
#                     "Northwood",
#                     "Northwood Hills",
#                     "Notting Hill Gate",
#                     "Oakwood",
#                     "Old Street",
#                     "Osterley",
#                     "Oval",
#                     "Oxford Circus",
#                     "Paddington",
#                     "Park Royal",
#                     "Parsons Green",
#                     "Perivale",
#                     "Piccadilly Circus",
#                     "Pimlico",
#                     "Pinner",
#                     "Plaistow",
#                     "Preston Road",
#                     "Putney Bridge",
#                     "Queensbury",
#                     "Queens Park",
#                     "Queensway",
#                     "Ravenscourt Park",
#                     "Rayners Lane",
#                     "Redbridge",
#                     "Regents Park",
#                     "Richmond",
#                     "Rickmansworth",
#                     "Roding Valley",
#                     "Rotherhithe",
#                     "Royal Oak",
#                     "Ruislip",
#                     "Ruislip Gardens",
#                     "Ruislip Manor",
#                     "Russell Square",
#                     "Seven Sisters",
#                     "Shadwell",
#                     "Shepherds Bush",
#                     "Shepherds Bush Market",
#                     "Shoreditch High Street",
#                     "Sloane Square",
#                     "Snaresbrook",
#                     "South Ealing",
#                     "Southfields",
#                     "Southgate",
#                     "South Harrow",
#                     "South Kensington",
#                     "South Kenton",
#                     "South Ruislip",
#                     "Southwark",
#                     "South Wimbledon",
#                     "South Woodford",
#                     "South Hampstead",
#                     "Stamford Brook",
#                     "Stanmore",
#                     "Star Lane",
#                     "Stepney Green",
#                     "St Jamess Park",
#                     "St Johns Wood",
#                     "Stockwell",
#                     "Stonebridge Park",
#                     "St Pauls",
#                     "Stratford",
#                     "Stratford High Street",
#                     "Stratford International",
#                     "Sudbury Hill",
#                     "Sudbury Town",
#                     "Surrey Quays",
#                     "Swiss Cottage",
#                     "Temple",
#                     "Theydon Bois",
#                     "Tooting Bec",
#                     "Tooting Broadway",
#                     "Tottenham Court Road",
#                     "Tottenham Hale",
#                     "Totteridge and Whetstone",
#                     "Tower Hill",
#                     "Tufnell Park",
#                     "Turnham Green",
#                     "Turnpike Lane",
#                     "Upminster",
#                     "Upminster Bridge",
#                     "Upney",
#                     "Upton Park",
#                     "Uxbridge",
#                     "Vauxhall",
#                     "Victoria",
#                     "Walthamstow Central",
#                     "Wanstead",
#                     "Wapping",
#                     "Warren Street",
#                     "Warwick Avenue",
#                     "Waterloo",
#                     "Watford",
#                     "Wembley Central",
#                     "Wembley Park",
#                     "Westbourne Park",
#                     "West Brompton",
#                     "West Finchley",
#                     "West Ham",
#                     "West Hampstead",
#                     "West Harrow",
#                     "West Kensington",
#                     "Westminster",
#                     "West Ruislip",
#                     "Whitechapel",
#                     "White City",
#                     "Willesden Green",
#                     "Willesden Junction",
#                     "Wimbledon",
#                     "Wimbledon Park",
#                     "Woodford",
#                     "Wood Green",
#                     "Woodside Park",
#                     "Custom House",
#                     "Greenwich",
#                     "Lewisham",
#                     "Limehouse",
#                     "All Saints",
#                     "Beckton",
#                     "Beckton Park",
#                     "Blackwall",
#                     "Bow Church",
#                     "Crossharbour",
#                     "Cutty Sark",
#                     "Cyprus",
#                     "Deptford Bridge",
#                     "Devons Road",
#                     "Langdon Park",
#                     "East India",
#                     "Elverson Road",
#                     "Gallions Reach",
#                     "Heron Quays",
#                     "Island Gardens",
#                     "Mudchute",
#                     "Poplar",
#                     "Prince Regent",
#                     "Pudding Mill Lane",
#                     "Royal Albert",
#                     "Royal Victoria",
#                     "South Quay",
#                     "Tower Gateway",
#                     "Westferry",
#                     "West India Quay",
#                     "West Silverton",
#                     "Pontoon Dock",
#                     "London City Airport",
#                     "King George V",
#                     "Woolwich Arsenal",
#                     "Clapham Junction",
#                     "South Acton",
#                     "Wood Lane",
#                     "Kensal Rise",
#                     "Kensington (Olympia)",
#                     "Brondesbury",
#                     "Brondesbury Park",
#                     "Finchley Road Frognal",
#                     "Hampstead Heath",
#                     "Gospel Oak",
#                     "Kentish Town West",
#                     "Camden Road",
#                     "Caledonian Road Barnsbury",
#                     "Canonbury",
#                     "Dalston Kingsland",
#                     "Hackney Central",
#                     "Homerton",
#                     "Hackney Wick",
#                     "Watford High Street",
#                     "Watford Junction",
#                     "Bushey",
#                     "Carpenders Park",
#                     "Hatch End",
#                     "Headstone Lane",
#                     "Upper Holloway",
#                     "Crouch Hill",
#                     "Harringay Green Lanes",
#                     "South Tottenham",
#                     "Walthamstow Queens Road",
#                     "Leyton Midland Road",
#                     "Leytonstone High Road",
#                     "Wanstead Park",
#                     "Woodgrange Park",
#                     "Kilburn High Road",
#                     "Acton Central",
#                     "West Croydon",
#                     "Norwood Junction",
#                     "Anerley",
#                     "Penge West",
#                     "Crystal Palace",
#                     "Sydenham",
#                     "Forest Hill",
#                     "Honor Oak Park",
#                     "Brockley",
#                     "Hoxton",
#                     "Haggerston",
#                     "Dalston Junction",
#                     "Wandsworth Road",
#                     "Clapham High Street",
#                     "Denmark Hill",
#                     "Peckham Rye",
#                     "Queens Road Peckham",
#                     "Emirates Greenwich Peninsula",
#                     "Emirates Royal Docks"
#     ]
#     unmapped_tube_stops = []
#
#     for k in tube_stops.keys():
#         if k.replace("Underground Station", "").strip() not in js_names:
#             unmapped_tube_stops.append(k)
#
#     print(unmapped_tube_stops)


if __name__ == "__main__":
    tube_stops = get_all_tube_stops_lines()
    write_webflow_cms(tube_stops)
    # verify_js_stop_names(tube_stops)

