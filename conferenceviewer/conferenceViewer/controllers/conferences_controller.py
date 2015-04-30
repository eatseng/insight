from __future__ import absolute_import

from clay import app
from flask import Blueprint, jsonify, request
from conferenceViewer.lib.errors_handler import InvalidUsage
from conferenceViewer.models.conference import Conference
from cassandra.cluster import Cluster

conferences_bp = Blueprint('conferences', __name__, url_prefix='/api', static_folder='../static/conferenceViewer')


@conferences_bp.route('/conferences')
def conferences():
    query_string = request.args
    list_for_day = query_string.get("list", None)
    conf_id = query_string.get("conf_id", None)

    if list_for_day is not None:
        conference = Conference()
        data = conference.get_conference_list(list_for_day)
    elif conf_id is not None:
        conference = Conference()
        data = conference.get_conference_detail(conf_id)
    else:
        # meeting_instance_id = return_input_query(query_string, "meeting_instance_id")

        # log = Log()
        # json = log.get_meeting_instance_data(meeting_instance_id)
        conference = Conference()
        data = conference.real_time("place_holder")
    # data = conference.get_conference_pictures("place_holder")

    return jsonify(conferences=data)


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
