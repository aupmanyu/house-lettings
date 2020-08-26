import json
import datetime
import psycopg2
from dateutil import parser
from flask import Flask, request, Response
from flask_cors import cross_origin
from flask_executor import Executor

from app import main, general_constants, rmv_constants

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
    webflow_form_number = int(criteria["name"][-1])
    user_data = criteria["data"]
    destinations = generate_destinations(user_data)
    try:
        date_low = datetime.datetime.strftime(parser.parse(user_data['Move In Date'].split('-')[0]),
                                               "%Y-%m-%d %H:%M:%S")
        date_high = datetime.datetime.strftime(parser.parse(user_data['Move In Date'].split('-')[1]),
                                               "%Y-%m-%d %H:%M:%S")
    except IndexError:
        date_high = date_low

    except parser.ParserError:
        date_high = date_low = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    if 'Email Address' not in user_data:
        find_user_email_query = """
        SELECT email from users
        WHERE webflow_form_number = %s
        """
        with psycopg2.connect(general_constants.DB_URL, sslmode='allow') as conn:
            with conn.cursor() as curs:
                curs.execute(find_user_email_query, (webflow_form_number,))
                data = curs.fetchone()
        email = data[0]
    else:
        email = user_data['Email Address']

    return {
        "email": email,
        "date_low": date_low,
        "date_high": date_high,
        "radius": 0,
        "maxPrice": float(user_data['Rent']),
        "minBedrooms": int(user_data['Number Of Bedrooms']),
        "keywords": generate_keywords(user_data),
        "desired_areas": generate_areas(user_data),
        "desired_cats": generate_cats(user_data),
        "destinations": [x for x in destinations],
        "webflow_form_number": webflow_form_number
    }


def generate_destinations(criteria):
    for i in range(1, 4):
        if criteria['Commute Destination {}'.format(i)]:
            modes = []
            commute_time = float(criteria['commute time {}'.format(i)].strip('<').strip('mins'))
            if criteria['Tube{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.transit.name: commute_time
                    })
            if criteria['Car{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.driving.name: commute_time
                    })
            if criteria['Walk{}'.format(i)] == 'true':
                modes.append(
                    {
                        rmv_constants.RmvTransportModes.walking.name: commute_time
                    })
            if criteria['Bicycling{}'.format(i)] == 'true':
                modes.append({
                    rmv_constants.RmvTransportModes.bicycling.name: commute_time
                })

            # if all([criteria['Tube{}'.format(i)] == 'false',
            #         criteria['Car{}'.format(i)] == 'false',
            #         criteria['Walk{}'.format(i)] == 'false',
            #         criteria['Bicycling{}'.format(i)] == 'false']):
            #     modes.append({
            #         rmv_constants.RmvTransportModes.transit.name: commute_time
            #     })

            yield {
                criteria['Commute Destination {}'.format(i)]: {
                    "modes": modes
                }
            }


def generate_areas(criteria):
    desired_areas = []
    for i in range(1, 4):
        if criteria['Select-Area-{}'.format(i)] != "--":
            desired_areas.append(criteria['Select-Area-{}'.format(i)])
    return desired_areas


def generate_cats(criteria):
    cats = [general_constants.NhoodCategorisation.best, general_constants.NhoodCategorisation.beautiful]
    if criteria['A Night Out'].lower() == 'true':
        cats.append(general_constants.NhoodCategorisation.nightlife)
    if criteria['5Star-Dining-Experience'].lower() == 'true':
        cats.extend([general_constants.NhoodCategorisation.eating, general_constants.NhoodCategorisation.restaurants])
    if criteria['Lots-Of-Green-Space'].lower() == 'true':
        cats.extend([general_constants.NhoodCategorisation.green, general_constants.NhoodCategorisation.village])
    if criteria['Shop-Till-You-Drop'].lower() == 'true':
        cats.append(general_constants.NhoodCategorisation.shopping)

    return cats


def generate_keywords(criteria):
    keywords = []
    if criteria["Wooden Floors"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.WOODEN_FLOORS)
    if criteria["Not On Ground Floor"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.NO_GROUND_FLOOR)
    if criteria["Open Plan Kitchen"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.OPEN_PLAN_KITCHEN)
    if criteria["Has Garden"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.GARDEN)
    if criteria["Close To Gym"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_GYM)
    if criteria["Not On Busy Street"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.NO_LOUD_STREET)
    if criteria["Park Nearby"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_PARK)
    if criteria["Is Bright"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.BRIGHT)
    if criteria["Modern Interiors"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.MODERN_INTERIORS)
    if criteria["Close To Supermarket"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PROXIMITY_SUPERMARKET)
    if criteria["Has Parking Space"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.PARKING_SPACE)
    if criteria["24hr Concierge"].lower() == 'true':
        keywords.append(general_constants.CheckboxFeatures.CONCIERGE)

    return keywords


if __name__ == '__main__':
    app.run(debug=True, use_debugger=False, use_reloader=True, passthrough_errors=True)
