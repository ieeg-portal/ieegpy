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


import xml.etree.ElementTree as ET
from deprecation import deprecated
from ieeg.dataset import Dataset as DS
from ieeg.ieeg_api import IeegApi

class Session:
    """
    Class representing Session on the platform. Session is context manager and can be used in `with` statements to automatically close resouces.

       with Session(username, password) as session:
           ...
    """
    host = "www.ieeg.org"
    port = ""
    method = 'https://'

    def __init__(self, name, pwd, verify_ssl=True, mprov_listener=None):
        self.username = name
        use_https = Session.method.startswith('https')
        # Session.url_builder requires Session.port == ':8080' to use port 8080.
        # But there shouldn't be anyone calling url_builder anyway.
        port = Session.port[1:] if Session.port.startswith(
            ':') else Session.port
        self.api = IeegApi(self.username, pwd,
                           use_https=use_https, host=Session.host, port=port, verify_ssl=verify_ssl)
        self.mprov_listener = mprov_listener

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


    def close(self):
        """
        Closes Session resources. Can also use Session as a context manager in a with clause:
            with Session(username, password) as session:
                ...
        """
        self.api.close()

    @deprecated
    def url_builder(self, path):
        return Session.method + Session.host + Session.port + path

    def _get_montages(self, dataset_id):
        """
        Returns the montages associated with this Dataset.
        """
        response = self.api.get_montages(dataset_id)
        response_body = response.json()
        # If there is just one montage, response will be a single
        # montage and not an array.
        single_montage_or_list = response_body['montages']['montage']
        json_montages = [single_montage_or_list] if isinstance(
            single_montage_or_list, dict) else single_montage_or_list
        return json_montages

    def open_dataset(self, name):
        """
        Return a dataset object
        """

        get_id_response = self.api.get_dataset_id_by_name(name)

        snapshot_id = get_id_response.text

        time_series_details_response = self.api.get_time_series_details(
            snapshot_id)

        json_montages = self._get_montages(snapshot_id)
        dataset = DS(name, ET.fromstring(
            time_series_details_response.text), snapshot_id, self, json_montages=json_montages)

        if self.mprov_listener:
            self.mprov_listener.on_open_dataset(name, dataset)

        return dataset

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
