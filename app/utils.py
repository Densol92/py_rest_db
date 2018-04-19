from datetime import datetime
from functools import wraps

import psycopg2
import dataset
from flask.globals import request

from settings import HOST, USER, DB_NAME, URI


def add_timestamp(records):
    now = datetime.now()
    for row in records:
        row['date'] = now


def check_auth(**kwargs):
    auth_table = get_table('tech', 'auth')
    return auth_table.find(**kwargs)


def get_table(schema, table_name):
    db = dataset.connect(URI, schema)
    table = db.get_table(table_name)
    return table


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        schema = auth.username
        if not auth or not check_auth(login=auth.username, password=auth.password):
            schema = 'test'
        return f(*args, **kwargs, schema=schema)

    return decorated


def create_schema(schema):
    db = psycopg2.connect(host=HOST, user=USER, dbname=DB_NAME)
    db.set_isolation_level(0)
    cursor = db.cursor()
    cursor.execute('CREATE SCHEMA %s' % schema)
    db.commit()
