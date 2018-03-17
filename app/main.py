import dataset
from flask import Flask, jsonify
from flask import request
from .settings import URI

app = Flask(__name__)
schema = None


@app.route('/<table_name>', methods=['POST'])
def insert(table_name):
    request_body = request.get_json()
    if isinstance(request_body, dict):
        request_body = [request_body]

    get_table(schema, table_name).insert_many(request_body)
    return '%s successfully stored in %s.%s' % (len(request_body), schema, table_name)


@app.route('/<table_name>', methods=['PUT'])
def update(table_name):
    request_body = request.get_json()
    keys = request.args.get('keys').split(',')

    updated = get_table(schema, table_name).update(request_body, keys)
    return '%s rows successfully updated in %s.%s' % (updated, schema, table_name)


@app.route('/<table_name>', methods=['GET'])
def select(table_name):
    return jsonify([row for row in get_table(schema, table_name).find(**request.args.to_dict())]), 200


@app.route('/<table_name>', methods=['DELETE'])
def delete(table_name):
    is_deleted = get_table(schema, table_name).delete(**request.args.to_dict())
    if is_deleted:
        return 'Rows successfully deleted in %s.%s by rule %s' % (schema, table_name, request.args.to_dict())
    return 'No rows to delete in %s.%s' % (schema, table_name)


@app.route('/health-check')
def health_check():
    return jsonify({"status": "OK", "message": "I'm ok."})


def get_table(schema, table_name):
    db = dataset.connect(URI, schema)
    table = db.get_table(table_name)
    return table

