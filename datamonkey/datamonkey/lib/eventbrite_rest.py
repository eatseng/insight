from __future__ import absolute_import

import requests


class EventBriteRest(object):

    def __init__(self, rest_endpoint=None, token=None):
        if rest_endpoint is None or not isinstance(rest_endpoint, str):
            raise ValueError('Must supply a rest_endpoint as a non empty string.')
        if token is None or not isinstance(token, str):
            raise ValueError('Must supply a token as a non empty string.')

        self.rest_endpoint = rest_endpoint
        self.token = token

    def search_events_by(self, keyword, start_time, end_time, page):
        url = self.rest_endpoint + "/v3/events/search"

        payload = {
            "format": "json",
            "q": keyword,
            "token": self.token,
            "page": page,
            "start_date.range_start": start_time,
            "start_date.range_end": end_time,
        }

        response = requests.get(url, params=payload)

        return response.json()
