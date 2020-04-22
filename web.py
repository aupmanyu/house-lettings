import json
import datetime
from dateutil import parser
from flask import Flask, request, Response
from flask_cors import cross_origin
from flask_executor import Executor

import main
import rmv_constants
import general_constants

app = Flask(__name__)
app.config['EXECUTOR_PROPAGATE_EXCEPTIONS'] = True  # To get errors from threads to surface up to console

executor = Executor(app)


@app.route('/', methods=['GET'])
def hello():
    return Response(json.dumps({"msg": "Here is the flask app!"}), status=200, mimetype='application/json')


@app.route('/search', methods=['POST'])
def search():
    user_config = generate_user_config(request.json)
    print(user_config)
    executor.submit(main.main, user_config)
    return Response(json.dumps({"msg": "Pipeline kicked off"}), status=202, mimetype='application/json')


@app.route('/update', methods=['POST'])
@cross_origin(allow_headers=['Content-Type'])
def update_prop_status():
    data = request.json
    print("Received data to update status of property {} to {}".format(data['slug'], data['status']))
    main.update_prop_status(data['slug'], data['status'])
    return Response(json.dumps({"msg": "Successfully updated status"}), status=200, mimetype='application/json')


def generate_user_config(criteria):
    destinations = generate_destinations(criteria)
    try:
        date_low = datetime.datetime.strftime(parser.parse(criteria['data__Move In Date'].split('-')[0]),
                                               "%Y-%m-%d %H:%M:%S")
        date_high = datetime.datetime.strftime(parser.parse(criteria['data__Move In Date'].split('-')[1]),
                                               "%Y-%m-%d %H:%M:%S")
    except IndexError:
        date_high = date_low

    except parser.ParserError:
        date_high = date_low = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    # TODO: store form number in DB too to avoid hardcoding here (which will require redeploy every time)
    if 'data__Email Address' not in criteria:
        mapping = {
            "Nectr Form1": "raduasandei@gmail.com"
        }
        email = mapping[criteria["name"]]
    else:
        email = criteria['data__Email Address']

    return {
        "email": email,
        "date_low": date_low,
        "date_high": date_high,
        "radius": 0,
        "maxPrice": float(criteria['data__Rent']),
        "minBedrooms": int(criteria['data__Number Of Bedrooms']),
        "keywords": generate_keywords(criteria),
        "desired_areas": generate_areas(criteria),
        "desired_cats": generate_cats(criteria),
        "destinations": [x for x in destinations]
    }


def generate_destinations(criteria):
    for i in range(1, 4):
        if criteria['data__Commute Destination {}'.format(i)]:
            modes = []
            commute_time = float(criteria['data__commute time {}'.format(i)].strip('<').strip('mins'))
            if criteria['data__Tube{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.transit.name: commute_time
                    })
            if criteria['data__Car{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.driving.name: commute_time
                    })
            if criteria['data__Walk{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.walking.name: commute_time
                    })
            if criteria['data__Bicycling{}'.format(i)] == 'true':
                modes.append({
                    rmv_constants.RmvTransportModes.bicycling.name: commute_time
                })

            yield {
                criteria['data__Commute Destination {}'.format(i)]: {
                    "modes": modes
                }
            }


def generate_areas(criteria):
    desired_areas = []
    for i in range(1, 4):
        if criteria['data__Select-Area-{}'.format(i)] != "--":
            desired_areas.append(criteria['data__Select-Area-{}'.format(i)])
    return desired_areas


def generate_cats(criteria):
    cats = [general_constants.NhoodCategorisation.best, general_constants.NhoodCategorisation.beautiful]
    if criteria['data__A Night Out'].lower() == 'true':
        cats.append(general_constants.NhoodCategorisation.nightlife)
    if criteria['data__5Star-Dining-Experience'].lower() == 'true':
        cats.extend([general_constants.NhoodCategorisation.eating, general_constants.NhoodCategorisation.restaurants])
    if criteria['data__Lots-Of-Green-Space'].lower() == 'true':
        cats.extend([general_constants.NhoodCategorisation.green, general_constants.NhoodCategorisation.village])
    if criteria['data__Shop-Till-You-Drop'].lower() == 'true':
        cats.append(general_constants.NhoodCategorisation.shopping)

    return cats


def generate_keywords(criteria):
    keywords = []
    if criteria["data__Wooden Floors"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.WOODEN_FLOORS)
    if criteria["data__Not On Ground Floor"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.NO_GROUND_FLOOR)
    if criteria["data__Open Plan Kitchen"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.OPEN_PLAN_KITCHEN)
    if criteria["data__Has Garden"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.GARDEN)
    if criteria["data__Close To Gym"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_GYM)
    if criteria["data__Not On Busy Street"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.NO_LOUD_STREET)
    if criteria["data__Park Nearby"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_PARK)
    if criteria["data__Is Bright"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.BRIGHT)
    if criteria["data__Modern Interiors"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.MODERN_INTERIORS)
    if criteria["data__Close To Supermarket"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_SUPERMARKET)
    if criteria["data__Has Parking Space"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PARKING_SPACE)
    if criteria["data__24hr Concierge"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.CONCIERGE)

    return keywords


if __name__ == '__main__':
    app.run(debug=True, use_debugger=False, use_reloader=True, passthrough_errors=True)
