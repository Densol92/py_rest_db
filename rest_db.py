from flask import Flask, jsonify
from flask import request
import dataset

app = Flask(__name__)


@app.route('/store-report', methods=['POST'])
def store_report():
    request_body = request.get_json()
    if isinstance(request_body, dict):
        request_body = [request_body]
    table_name = request.args['table']
    schema = 'public'
    inserted = len(store(schema, table_name, request_body))
    return '%s successfully stored in %s.%s' % (len(request_body), schema, table_name)


@app.route('/health-check')
def health_check():
    return jsonify({"status": "OK", "message": "I'm ok."})


def store(schema, table_name, data):
    from settings import URI
    db = dataset.connect(URI, schema)
    table = db.get_table(table_name)
    return table.insert(data)
