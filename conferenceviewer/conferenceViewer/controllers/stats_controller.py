from __future__ import absolute_import

from clay import app
from flask import Blueprint, jsonify, request
from conferenceViewer.lib.errors_handler import InvalidUsage
from conferenceViewer.models.stats import Stats

stats_bp = Blueprint('stats', __name__, url_prefix='/api', static_folder='../static/conferenceViewer')


@stats_bp.route('/stats')
def stats():
    query_string = request.args
    real_time = query_string.get("real_time", None)
    time_stamp = query_string.get("time_stamp", None)

    if real_time:
        stats = Stats()
        data = stats.get_updates(time_stamp)
    else:
        stats = Stats()
        data = stats.get_map()

    return jsonify(stats=data)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def return_input_query(query_string, field, is_num=True):
    if query_string.get(field, None) is not None:
        field_value = int(query_string.get(field)) if is_num else str(query_string.get(field))
    else:
        raise InvalidUsage('Missing ' + field, status_code=400)
    return field_value
