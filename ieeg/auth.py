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
import base64
import datetime
import requests
import xml.etree.ElementTree as ET
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from ieeg.dataset import Dataset as DS, IeegConnectionError
from deprecation import deprecated

class Session:
    """
    Class representing Session on the platform
    """
    host = "www.ieeg.org"
    port = ""
    method = 'https://'

    username = ""
    password = ""

    def __init__(self, name, pwd):
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.username = name
        self.password = md5(pwd)

    def url_builder(self, path):
        return Session.method + Session.host + Session.port + path

    def create_ws_header(self, path, http_method, query=None, payload="", request_json=False):
        d_time = datetime.datetime.now().isoformat()
        sig = self._signature_generator(path, http_method, d_time, query, payload)
        headers = {'username': self.username, \
                'timestamp': d_time, \
                'signature': sig, \
                'Content-Type': 'application/xml'}
        if (request_json):
            headers['Accept'] = 'application/json'
        return headers

    def _signature_generator(self, path, http_method, d_time, query=None, payload=""):
        """
        Signature Generator, used to authenticate user in portal
        """

        m = hashlib.sha256()

        query_str = ""
        if query:
            for k, v in query.items():
                query_str += k + "=" + str(v) + "&"
            query_str = query_str[0:-1]

        m.update(payload.encode('utf-8'))
        payload_hash = base64.standard_b64encode(m.digest())

        to_be_hashed = (self.username + "\n" +
                        self.password + "\n" +
                        http_method + "\n" +
                        self.host + "\n" +
                        path + "\n" +
                        query_str + "\n" +
                        d_time + "\n" +
                        payload_hash.decode('utf-8'))

        m2 = hashlib.sha256()
        m2.update(to_be_hashed.encode('utf-8'))
        return base64.standard_b64encode(m2.digest())

    def open_dataset(self, name):
        """
        Return a dataset object
        """

        # Request location
        get_id_by_snap_name_path = "/services/timeseries/getIdByDataSnapshotName/"

        # Create request content
        http_method = "GET"
        req_path = get_id_by_snap_name_path + name
        payload = self.create_ws_header(req_path, http_method, "", "")
        url = Session.method + Session.host + Session.port + req_path

        r = requests.get(url, headers=payload, verify=False)

        # Check response
        if r.status_code != 200:
            print(r.text)
            raise IeegConnectionError('Authorization failed or cannot find study ' + name)

        # Request location
        get_time_series_url = '/services/timeseries/getDataSnapshotTimeSeriesDetails/'

        # Create request content
        snapshot_id = r.text
        req_path = get_time_series_url + snapshot_id
        payload = self.create_ws_header(req_path, http_method, "", "")
        url = Session.method + Session.host + Session.port + req_path
        r = requests.get(url, headers=payload, verify=False)

        # Return Habitat Dataset object
        return DS(ET.fromstring(r.text), snapshot_id, self)

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
        return Session.url_builder(path)

    @deprecated
    def openDataset(self, name):
        return self.open_dataset(name)

    @deprecated
    def _createWSHeader(self, path, http_method, query, payload):
        return self.create_ws_header(path, http_method, query, payload)


def md5(user_string):
    """
    Return MD5 hashed string
    """
    m = hashlib.md5()
    m.update(user_string.encode('utf-8'))
    return m.hexdigest()


