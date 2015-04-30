from __future__ import absolute_import

from flask import Blueprint, render_template

conferenceViewer_bp = Blueprint('conferenceViewer', __name__, static_folder='../static/conferenceViewer')


@conferenceViewer_bp.route('/')
def overview():
    return render_template('conferenceviewer/conferenceviewer.html')
