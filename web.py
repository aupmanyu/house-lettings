from flask import Flask, escape, request
import main

from flask_executor import Executor

app = Flask(__name__)

executor = Executor(app)

@app.route('/', methods=['GET'])
def hello():
    return "Here is the flask app!"


@app.route('/search', methods=['POST'])
def search():
    criteria = request.json
    print(criteria['radius'])
    executor.submit(main.main, criteria)
    return "Thanks", 202


if __name__ == '__main__':
    app.run(debug=True, use_debugger=False, use_reloader=False, passthrough_errors=True)
