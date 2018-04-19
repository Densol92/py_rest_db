import random
import string

from flask import Flask, jsonify
from flask import request
from datetime import datetime

from utils import get_table, add_timestamp, check_auth, requires_auth

app = Flask(__name__)


@app.route('/<table_name>', methods=['POST'])
@requires_auth
def insert(table_name, schema):
    request_body = request.get_json()
    if isinstance(request_body, dict):
        request_body = [request_body]
    add_timestamp(request_body)

    get_table(schema, table_name).insert_many(request_body)
    return '%s successfully stored in %s.%s' % (len(request_body), schema, table_name)


@app.route('/<table_name>', methods=['PUT'])
@requires_auth
def update(table_name, schema):
    request_body = request.get_json()
    keys = request.args.get('keys').split(',')

    updated = get_table(schema, table_name).update(request_body, keys)
    return '%s rows successfully updated in %s.%s' % (updated, schema, table_name)


@app.route('/<table_name>', methods=['GET'])
@requires_auth
def select(table_name, schema):
    return jsonify([row for row in get_table(schema, table_name).find(**request.args.to_dict())]), 200


@app.route('/<table_name>', methods=['DELETE'])
@requires_auth
def delete(table_name, schema):
    is_deleted = get_table(schema, table_name).delete(**request.args.to_dict())
    if is_deleted:
        return 'Rows successfully deleted in %s.%s by rule %s' % (schema, table_name, request.args.to_dict())
    return 'No rows to delete in %s.%s' % (schema, table_name)


@app.route('/health-check')
def health_check():
    return jsonify({"status": "OK", "message": "I'm ok."})


@app.route('/register')
def register():
    login = request.args.get('login')
    if not login:
        return '/register?login=YOUR_LOGIN'
    auth_table = get_table('tech', 'auth')
    existed = check_auth(login=login)
    if not existed:
        existed = {'login': login, 'password': ''.join(random.choice(string.ascii_letters) for _ in range(10))}

        auth_table.insert(existed)
    else:
        existed = [row for row in existed][0]
    return jsonify(existed)


# create schema from execute, not dataset. PsqlDB.connect_to_report_db().execute_sql('CREATE SCHEMA test2')

if __name__ == '__main__':
    print(get_table('test2', 'other_test').insert_many([
        {'name': 'odin', 'date': datetime.today()},
        {'name': 'dva', 'date': datetime.today()},
    ]))
