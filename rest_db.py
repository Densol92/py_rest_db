from flask import Flask, jsonify
from flask import request

app = Flask(__name__)


@app.route('/store-report', methods=['POST'])
def store_report():
    request_body = request.get_json()
    if isinstance(request_body, dict):
        request_body = [request_body]
    table_name = request.args['table']
    schema = 'public'

    return '%s successfully stored in %s.%s' % (len(request_body), schema, table_name)


@app.route('/health-check')
def health_check():
    return jsonify({"status": "OK", "message": "I'm ok."})


class ErrorStorage(object):
    def __init__(self, uri=None):
        if uri is None:
            uri = 'sqlite:////tmp/test_error_storage.db'
        self.db = dataset.connect(uri)
        self.test_results = self.db.get_table('test_results')

    def get_execution_result(self, tag):
        return [row for row in self.test_results.find(execution_tag=tag)]

    def report_result(self, case_id, run_id, status, exception, tag):
        row = dict(case_id=case_id, run_id=run_id, status=status, exception=exception, execution_tag=tag)
        if status == Status.PASSED:
            self.test_results.update(row, ['execution_tag', 'case_id', 'run_id'])
        else:
            self.test_results.upsert(row, ['execution_tag', 'case_id', 'run_id'])