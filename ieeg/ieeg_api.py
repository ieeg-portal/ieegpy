'''
 Copyright 2019 Trustees of the University of Pennsylvania

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''
import xml.etree.ElementTree as ET
import requests
from ieeg.ieeg_auth import IeegAuth


class IeegApi:
    """
    The IEEG REST API
    """

    _get_id_by_dataset_name_path = "/timeseries/getIdByDataSnapshotName/"
    _get_time_series_details_path = '/timeseries/getDataSnapshotTimeSeriesDetails/'
    _get_counts_by_layer_path = '/timeseries/getCountsByLayer/'
    _get_annotations_path = '/timeseries/getTsAnnotations/'
    _get_data_path = '/timeseries/getUnscaledTimeSeriesSetBinaryRaw/'
    _derive_dataset_path = '/timeseries/deriveDataSnapshotFull/'
    _get_montages_path = '/datasets/%s/montages'
    _add_annotations_path = '/timeseries/addAnnotationsToDataSnapshot/'
    _delete_annotation_layer_path = '/timeseries/removeTsAnnotationsByLayer/'

    _send_json = {'Content-Type': 'application/json'}
    _send_xml = {'Content-Type': 'application/xml'}
    _accept_json = {'Accept': 'application/json'}
    _accept_xml = {'Accept': 'application/xml'}
    _send_accept_json = {
        'Content-Type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, username, password,
                 use_https=True, host='www.ieeg.org', port=None, verify_ssl=True):
        self.http = requests.Session()
        self.http.auth = IeegAuth(username, password)
        self.http.verify = verify_ssl
        self.scheme = 'https' if use_https else 'http'
        self.host = host
        self.port = port
        authority = host + ':' + str(port) if port else host
        self.base_url = self.scheme + '://' + authority + '/services'

    def close(self):
        """
        Closes HTTP resources
        """
        self.http.close()

    def get_dataset_id_by_name(self, dataset_name):
        """
        Returns a Response with a dataset's id given its name
        """
        url = self.base_url + IeegApi._get_id_by_dataset_name_path + dataset_name

        response = self.http.get(url)
        return response

    def get_time_series_details(self, dataset_id):
        """
        Returns Response with time series details in XML format
        """
        url = self.base_url + IeegApi._get_time_series_details_path + dataset_id
        response = self.http.get(url, headers=IeegApi._accept_xml)
        return response

    def get_annotation_layers(self, dataset):
        """
        Returns Response with Annotation layers and counts in JSON format.
        """
        url_str = self.base_url + IeegApi._get_counts_by_layer_path + dataset.snap_id

        response = self.http.get(url_str, headers=IeegApi._accept_json)
        return response

    def get_annotations(self, dataset, layer_name,
                        start_offset_usecs=None, first_result=None, max_results=None):
        """
        Returns a Response containing a JSON formatted list of annotations in the given
        layer ordered by start time.

        Given a Dataset ds with no new annotations being added, if ds.get_annotations('my_layer')
        returns 152 annotations, then ds.get_annotations('my_layer', max_results=100) will return
        the first 100 of those and ds.get_annotations('my_layer', first_result=100, max_results=100)
        will return the final 52.

        :param layer_name: The annotation layer to return
        :param start_offset_usec:
               If specified all returned annotations will have a start offset >= start_offset_usec
        :param first_result: If specified, the zero-based index of the first annotation to return.
        :param max_results: If specified, the maximum number of annotations to return.
        :returns: a list of annotations in the given layer ordered by start offset.
        """
        url_str = self.base_url + IeegApi._get_annotations_path + \
            dataset.snap_id + '/' + layer_name

        params = {'startOffsetUsec': start_offset_usecs,
                  'firstResult': first_result, 'maxResults': max_results}

        response = self.http.get(
            url_str, headers=IeegApi._accept_json, params=params)
        return response

    def derive_dataset(self, dataset, derived_dataset_name, tool_name):
        """
        Returns a Response containing the portal id of a new Dataset.

        The new Dataset will have the name given in the derived_dataset_name
        and be a copy of the given dataset.

        :param dataset: The dataset to copy
        :param derived_dataset_name: The name of the new dataset
        :param tool_name: The name of the tool creating the new dataset
        :returns: the portal id of the new dataset
        """
        url_str = self.base_url + IeegApi._derive_dataset_path + dataset.snap_id

        params = {'friendlyName': derived_dataset_name,
                  'toolName': tool_name}

        response = self.http.post(
            url_str, headers=IeegApi._accept_json, params=params)
        return response

    def get_data(self, dataset, start, duration, channels):
        """
        Returns data from the IEEG platform
        :param start: Start time (usec)
        :param duration: Number of usec to request samples from
        :param channels: Integer indices of the channels we want
        :return: a Response with binary content.
        """
        # Build Data Content XML
        wrapper1 = ET.Element('timeSeriesIdAndDChecks')
        wrapper2 = ET.SubElement(wrapper1, 'timeSeriesIdAndDChecks')
        i = 0
        for ts_details in dataset.ts_array:
            if i in channels:
                el1 = ET.SubElement(wrapper2, 'timeSeriesIdAndCheck')
                el2 = ET.SubElement(el1, 'dataCheck')
                el2.text = ts_details.findall('dataCheck')[0].text
                el3 = ET.SubElement(el1, 'id')
                el3.text = ts_details.findall('revisionId')[0].text
            i += 1

        data = ET.tostring(wrapper1, encoding="us-ascii",
                           method="xml").decode('utf-8')
        data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>' + data

        params = {'start': start, 'duration': duration}
        url_str = self.base_url + IeegApi._get_data_path + dataset.snap_id

        response = self.http.post(url_str,
                                  params=params, data=data, headers=IeegApi._send_xml)
        return response

    def get_montages(self, dataset_id):
        """
        Returns the montages for the given dataset.
        """
        url_str = self.base_url + IeegApi._get_montages_path % dataset_id

        response = self.http.get(url_str, headers=IeegApi._accept_json)
        return response

    def add_annotations(self, dataset, annotations):
        """
        Adds annotations to the given snapshot.
        :returns: a Response with String body (the datset id)
        """
        # request_body is oddly verbose because it was originally designed as XML.
        ts_revids = set()
        ts_annotations = []
        for annotation in annotations:
            if annotation.parent != dataset:
                raise ValueError(
                    'Annotation does not belong to this dataset. It belongs to dataset '
                    + annotation.parent.snap_id)
            annotated_revids = [
                detail.portal_id for detail in annotation.annotated]
            ts_annotation = {
                'timeseriesRevIds': {'timeseriesRevId': annotated_revids},
                'annotator': annotation.annotator,
                'type': annotation.type,
                'description': annotation.description,
                'layer': annotation.layer,
                'startTimeUutc': annotation.start_time_offset_usec,
                'endTimeUutc': annotation.end_time_offset_usec
            }
            if annotation.portal_id:
                ts_annotation['revId'] = annotation.portal_id
            ts_annotations.append(ts_annotation)
            ts_revids.update(annotated_revids)

        timeseries = [{'revId': ts_revid, 'label': dataset.ts_details_by_id[ts_revid].channel_label}
                      for ts_revid in ts_revids]
        request_body = {'timeseriesannotations': {
            'timeseries': {
                'timeseries': timeseries
            },
            'annotations': {
                'annotation': ts_annotations
            }
        }}
        url_str = self.base_url + IeegApi._add_annotations_path + dataset.snap_id
        response = self.http.post(url_str,
                                  json=request_body,
                                  headers=IeegApi._send_json)
        return response

    def move_annotation_layer(self, dataset, from_layer, to_layer):
        """
        Moves annotations in the given dataset from from_layer to to_layer.

        :returns: a Response with JSON body. Has number of moved annotations.
        """
        req_path = ('/timeseries/datasets/'
                    + dataset.snap_id
                    + '/tsAnnotations/'
                    + from_layer)
        url_str = self.base_url + req_path

        query_params = {'toLayerName': to_layer}

        response = self.http.post(
            url_str, params=query_params, headers=IeegApi._accept_json)
        return response

    def delete_annotation_layer(self, dataset, layer):
        """
        Deletes annotations in layer from the given dataset.

        :returns: a Response with JSON body. Has number of delelted annotations.
        """
        url_str = self.base_url + IeegApi._delete_annotation_layer_path + \
            dataset.snap_id + '/' + layer

        response = self.http.post(url_str, headers=IeegApi._accept_json)
        return response
