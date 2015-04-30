from __future__ import absolute_import

import requests
import xml.etree.ElementTree as ET


class FlickrRest(object):

    def __init__(self, rest_endpoint=None, api_key=None):
        if rest_endpoint is None or not isinstance(rest_endpoint, str):
            raise ValueError('Must supply a rest_endpoint as a non empty string.')
        if api_key is None or not isinstance(api_key, str):
            raise ValueError('Must supply a token as a non empty string.')

        self.rest_endpoint = rest_endpoint
        self.api_key = api_key

    def get_photos_by_keyword(self, keyword):
        method = "flickr.photos.search"

        payload = {
            "method": method,
            "text": keyword,
            "sort": "interestingness-desc",
            "api_key": self.api_key
        }

        response = requests.get(self.rest_endpoint, params=payload)

        photo_urls = self.get_photo_urls(response)

        return photo_urls

    def get_photo_urls(self, response):

        root = ET.fromstring(response.content)

        # https://farm{farm-id}.staticflickr.com/{server-id}/{id}_{o-secret}_o.(jpg|gif|png)

        photo_urls = []

        photos = root.find("photos")

        for photo in photos.findall('photo'):
            farm_id = photo.get("farm")
            server_id = photo.get("server")
            id = photo.get("id")
            o_secret = photo.get("secret")
            photo_urls.append("https://farm" + str(farm_id) + ".staticflickr.com/" + str(server_id) + "/" + str(id) + "_" + str(o_secret) + "_n.jpg")

        return photo_urls
