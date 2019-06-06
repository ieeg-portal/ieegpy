##################################################################################
# Copyright 2013-19 by the Trustees of the University of Pennsylvania
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################


import hashlib
import xml.etree.ElementTree as ET
import requests
from deprecation import deprecated
from ieeg.dataset import Dataset as DS, IeegConnectionError
from ieeg.ieeg_auth import IeegAuth
from ieeg.ieeg_api import IeegApi

class Session:
    """
    Class representing Session on the platform
    """
    host = "www.ieeg.org"
    port = ""
    method = 'https://'

    def __init__(self, name, pwd):
        self.username = name
        self.password = md5(pwd)
        self.auth_for_json_req = IeegAuth(self.username, self.password, request_json=True)
        self.auth_for_xml_req = IeegAuth(self.username, self.password)
        self.http = requests.Session()
        self.api = IeegApi(self.http, self.auth_for_xml_req)

    def close(self):
        self.api.close()

    def url_builder(self, path):
        return Session.method + Session.host + Session.port + path

    def open_dataset(self, name):
        """
        Return a dataset object
        """

        get_id_response = self.api.get_dataset_id_by_name(name)

        # Check response
        if get_id_response.status_code != 200:
            print(get_id_response.text)
            raise IeegConnectionError('Authorization failed or cannot find study ' + name)

        snapshot_id = get_id_response.text

        time_series_details_response = self.api.get_time_series_details(snapshot_id)

        if time_series_details_response.status_code != 200:
            print(time_series_details_response.text)
            raise IeegConnectionError('Authorization failed or cannot get time series details for ' + name)

        # Return Habitat Dataset object
        return DS(ET.fromstring(time_series_details_response.text), snapshot_id, self)

    def close_dataset(self, ds):
        """
        Close connection (for future use)
        :param ds: Dataset to close
        :return:
        """
        return

    # For backward-compatibility
    @deprecated
    def urlBuilder(self, path):
        return self.url_builder(path)

    @deprecated
    def openDataset(self, name):
        return self.open_dataset(name)

def md5(user_string):
    """
    Return MD5 hashed string
    """
    m = hashlib.md5()
    m.update(user_string.encode('utf-8'))
    return m.hexdigest()


