import datetime
from dateutil import parser
from flask import Flask, escape, request
from flask_cors import cross_origin
from flask_executor import Executor

import main
import rmv_constants

app = Flask(__name__)
app.config['EXECUTOR_PROPAGATE_EXCEPTIONS'] = True

executor = Executor(app)



@app.route('/', methods=['GET'])
def hello():
    return "Here is the flask app!"


@app.route('/search', methods=['POST'])
def search():
    user_config = generate_user_config(request.json)
    print(user_config)
    executor.submit(main.main, user_config)
    return "Thanks", 202


@app.route('/update', methods=['POST'])
@cross_origin()
def update_prop_status():
    data = request.json
    print(data)
    return "Success", 200


def generate_user_config(criteria):
    destinations = generate_destinations(criteria)
    return {
        "email": criteria['data__Email Address'],
        "date_low": datetime.datetime.strftime(parser.parse(criteria['data__Move In Date'].split('-')[0]),
                                               "%Y-%m-%d %H:%M:%S"),
        "date_high": datetime.datetime.strftime(parser.parse(criteria['data__Move In Date'].split('-')[1]),
                                                "%Y-%m-%d %H:%M:%S"),
        "radius": 0,
        "maxPrice": float(criteria['data__Rent']),
        "minBedrooms": int(criteria['data__Number Of Bedrooms']),
        "keywords": [],
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


if __name__ == '__main__':
    app.run(debug=True, use_debugger=False, use_reloader=False, passthrough_errors=True)
