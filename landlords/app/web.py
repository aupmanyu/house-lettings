import json
from flask import Flask, request, Response

from . import main

app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello():
    return Response(json.dumps({"msg": "Here is the flask app!"}), status=200, mimetype='application/json')


@app.route('/manage-bookings', methods=['POST'])
def manage_booking():
    data = request.json()
    main.booking_handler(data)
    return Response(json.dumps({"msg": "Accepted your request"}), status=202, mimetype="application/json")


if __name__ == '__main__':
    app.run(debug=True, use_debugger=False, use_reloader=True, passthrough_errors=True)