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
import json
import requests
import numpy as np
import pandas as pd
from deprecation import deprecated


class TimeSeriesDetails:
    """
    Metadata on a given time series
    """
    portal_id = ""
    acquisition = ""
    name = ""
    duration = 0.
    channel_label = ""
    min_sample = 0
    max_sample = 0
    number_of_samples = 0
    start_time = 0
    voltage_conversion_factor = 1.

    def __init__(self, portal_id, name, label, duration, min_sample, max_sample, number_of_samples, start_time, voltage_conversion):
        self.portal_id = portal_id
        self.name = name
        self.channel_label = label
        self.duration = float(duration)
        self.min_sample = int(min_sample)
        self.max_sample = int(max_sample)
        self.number_of_samples = int(number_of_samples)
        self.start_time = int(start_time)
        self.voltage_conversion_factor = float(voltage_conversion)
        return

    def __str__(self):
        return self.name + "(" + self.channel_label + ") spans " + \
            str(self.duration) + " usec, range [" + str(self.min_sample) + "-" + \
            str(self.max_sample) + "] in " + str(self.number_of_samples) + \
            str(self.number_of_samples) + " samples.  Starts @" + str(self.start_time) + \
            " with voltage conv factor " + str(self.voltage_conversion_factor)


class Annotation:
    """
    A timeseries annotation on the platform

    Attributes:
        parent: The Dataset to which this Annotation belongs
        portal_id: The id of this Annotation
        annotated: List of TimeSeriesDetails annotated by this Annotation
        annotator: The creator of this Annotation
        type: The type
        description: The description
        layer: The layer
        start_time_offset_usec: The start time of this Annotations.
                                In microseconds since the recording start.
        end_time_offset_usec: The end time of this Annotation. 
                              In microseconds since the recording start.
    """

    def __init__(self, parent_dataset, annotator, _type, description, layer,
                 start_time_offset_usec, end_time_offset_usec,
                 portal_id=None, annotated_labels=None, annotated_portal_ids=None):
        """
        Creates an Annotation.

        Only one of annotated_labels or annotated_portal_ids need to be provided.
        If neither is provided, then this Annotation will annotate all the channels
        in parent_dataset.

        Args:
            :param parent_dataset: The Dataset to which this Annotation belongs.
            :param annotator: The Annotation's creator
            :param _type: The Annotation's type
            :param description: The Annotation's description
            :param layer: The Annotation's layer
            :param start_time_offset_usec: The Annotation's start time.
                                           In microseconds since the start of recording
            :param end_time_offset_usec: The Annotation's end time.
                                         In microseconds since the start of recording
            :param portal_id: The Annotation's id. Should be left as None for new Annotations.
            :param annotated_labels: The labels of the annotated TimeSeriesDetails.
                                     Either a string or list of strings.
            :param annotated_portal_ids: The portal_ids of the annotated TimeSeriesDetails.
                                         Either a string or list of strings/
        """
        self.parent = parent_dataset
        self.portal_id = str(portal_id) if portal_id else None
        if annotated_labels:
            self.annotated = [self.parent.ts_details[label] for label in (
                [annotated_labels] if isinstance(annotated_labels, str) else annotated_labels)]
        elif annotated_portal_ids:
            self.annotated = [self.parent.ts_details_by_id[rev_id] for rev_id in (
                [annotated_portal_ids] if isinstance(annotated_portal_ids, str) else annotated_portal_ids)]
        else:
            self.annotated = list(self.parent.ts_details.values())
        self.annotator = annotator
        self.type = _type
        self.description = description
        self.layer = layer
        self.start_time_offset_usec = start_time_offset_usec
        self.end_time_offset_usec = end_time_offset_usec

    def __repr__(self):
        return "annotation(" + self.portal_id + "): " + self.type + "(" + str(self.start_time_offset_usec) + ", " + str(self.end_time_offset_usec) + ")"


class Dataset:
    """
    Class representing Dataset on the platform
    """
    snap_id = ""
    ts_details = {}  # Time series details by label
    ts_details_by_id = {}  # Time series details by portal_id
    ch_labels = []  # Channel Labels
    ts_array = []   # Channel

    def __init__(self, ts_details, snapshot_id, parent):
        # type: (xml.etree.Element, str, ieeg.auth.Session) -> None
        self.session = parent
        self.snap_id = snapshot_id
        # only one details in timeseriesdetails
        details = ts_details.findall('details')[0]

        for dt in details.findall('detail'):
            # ET.dump(dt)
            name = dt.findall('channelLabel')[0].text
            portal_id = dt.findall('revisionId')[0].text
            details = TimeSeriesDetails(portal_id,
                                        dt.findall('name')[0].text,
                                        name,
                                        dt.findall('duration')[0].text,
                                        dt.findall('minSample')[0].text,
                                        dt.findall('maxSample')[0].text,
                                        dt.findall('numberOfSamples')[0].text,
                                        dt.findall('startTime')[0].text,
                                        dt.findall('voltageConversionFactor')[0].text,)
            self.ch_labels.append(name)
            self.ts_array.append(dt)
            self.ts_details[name] = details
            self.ts_details_by_id[portal_id] = details

    def __repr__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."

    def __str__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."

    def get_channel_labels(self):
        return self.ch_labels

    def get_channel_indices(self, list_of_labels):
        """
        Create a list of indices (suitable for get_data) from a list
        of labels
        :param list_of_labels: Ordered list of channel labels
        :return: Ordered list of channel indices
        """
        return [self.ch_labels.index(x) for x in list_of_labels]

    def get_time_series_details(self, label):
        """
        Get the metadata on a channel

        :param label: Channel label
        :return: object of type TimeSeriesDetails
        """
        return self.ts_details[label]

    def get_data(self, start, duration, channels):
        """
        Returns data from the IEEG platform
        :param start: Start time (usec)
        :param duration: Number of usec to request samples from
        :param channels: Integer indices of the channels we want
        :return: 2D array, rows = samples, columns = channels
        """

        def all_same(items):
            return all(x == items[0] for x in items)

        # Request location
        get_data_str = "/services/timeseries/getUnscaledTimeSeriesSetBinaryRaw/"

        # Build Data Content XML
        wrapper1 = ET.Element('timeSeriesIdAndDChecks')
        wrapper2 = ET.SubElement(wrapper1, 'timeSeriesIdAndDChecks')
        i = 0
        for ts in self.ts_array:
            if i in channels:
                el1 = ET.SubElement(wrapper2, 'timeSeriesIdAndCheck')
                el2 = ET.SubElement(el1, 'dataCheck')
                el2.text = ts.findall('dataCheck')[0].text
                el3 = ET.SubElement(el1, 'id')
                el3.text = ts.findall('revisionId')[0].text
            i += 1

        data = ET.tostring(wrapper1, encoding="us-ascii",
                           method="xml").decode('utf-8')
        data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>' + data

        # Create request content
        req_path = get_data_str + self.snap_id
        http_method = "POST"
        params = {'start': start, 'duration': duration}
        payload = self.session.create_ws_header(
            req_path, http_method, params, data)
        url_str = self.session.url_builder(req_path)

        # response to request
        r = requests.post(url_str, headers=payload,
                          params=params, data=data, verify=False)
        # collect data in numpy array
        d = np.fromstring(r.content, dtype='>i4')
        h = r.headers

        # Check all channels are the same length
        sample_per_row = [int(numeric_string)
                          for numeric_string in r.headers['samples-per-row'].split(',')]
        if not all_same(sample_per_row):
            raise IeegConnectionError(
                'Not all channels in response have equal length')

        conv_f = np.array([float(numeric_string)
                           for numeric_string in r.headers['voltage-conversion-factors-mv'].split(',')])

        # Reshape to 2D array and Multiply by conversionFactor
        d2 = np.reshape(d, (-1, len(sample_per_row))) * conv_f[np.newaxis, :]

        return d2

    def get_dataframe(self, start, duration, channels):
        """
        Returns data from the IEEG platform
        :param start: Start time (usec)
        :param duration: Number of usec to request samples from
        :param channels: Integer indices of the channels we want
        :return: dataframe, rows = samples, columns = labeled channels
        """

        array = self.get_data(start, duration, channels)
        return pd.DataFrame(array, columns=[self.ch_labels[i] for i in channels])

    def get_annotation_layers(self):
        """
        Returns a dictionary mapping layer names to annotation count for this Dataset.

        :returns: a dictionary which maps layer names to annotation count for layer
        """

        # Create request content
        req_path = "/services/timeseries/getCountsByLayer/" + self.snap_id
        http_method = "GET"
        payload = self.session.create_ws_header(
            req_path, http_method, request_json=True)
        url_str = self.session.url_builder(req_path)

        # response to request
        r = requests.get(url_str, headers=payload, verify=False)
        response_body = r.json()
        if r.status_code != requests.codes.ok:
            print(response_body)
            raise IeegConnectionError(
                'Could not get annotation layers for dataset')
        counts_by_layer = response_body['countsByLayer']['countsByLayer']
        if not counts_by_layer:
            return {}
        # If there is one layer, entry will be a dictionary.
        # If there is more than one, entry will be a list of dictionaries.
        entry = counts_by_layer['entry']
        try:
            return {entry['key']: entry['value']}
        except TypeError:
            return {e['key']: e['value'] for e in entry}

    def get_annotations(self, layer_name, start_offset_usecs=None, first_result=None, max_results=None):
        """
        Returns a list of annotations in the given layer ordered by start time.

        Given a Dataset ds with no new annotations being added, if ds.get_annotations('my_layer') returns 152 annotations,
        then ds.get_annotations('my_layer', max_results=100) will return the first 100 of those and 
        ds.get_annotations('my_layer', first_result=100, max_results=100) will return the final 52.

        :param layer_name: The annotation layer to return
        :param start_offset_usec: If specified all returned annotations will have a start offset >= start_offset_usec
        :param first_result: If specified, the zero-based index of the first annotation to return.
        :param max_results: If specified, the maximum number of annotations to return.
        :returns: a list of annotations in the given layer ordered by start offset.
        """

        req_path = '/services/timeseries/getTsAnnotations/' + \
            self.snap_id + '/' + layer_name
        http_method = "GET"
        params = {'startOffsetUsec': start_offset_usecs,
                  'firstResult': first_result, 'maxResults': max_results}

        payload = self.session.create_ws_header(
            req_path, http_method, query=params, request_json=True)
        url_str = self.session.url_builder(req_path)
        r = requests.get(url_str, headers=payload, verify=False, params=params)
        if r.status_code != requests.codes.ok:
            print(r.text)
            raise IeegConnectionError(
                'Could not get annotation layer ' + layer_name)
        response_body = r.json()
        timeseries_annotations = response_body['timeseriesannotations']
        json_annotations = timeseries_annotations['annotations']['annotation']

        try:
            annotations = [Annotation(
                self,
                a['annotator'],
                a['type'],
                a.get('description', ''),
                a['layer'],
                a['startTimeUutc'],
                a['endTimeUutc'],
                portal_id=a['revId'],
                annotated_portal_ids=a['timeseriesRevIds']['timeseriesRevId'])
                for a in json_annotations]
        except TypeError:
            # If there is only one annotation in the layer,
            # json_annotations won't be a list. It'll be an annotation.
            annotations = [Annotation(
                self,
                json_annotations['annotator'],
                json_annotations['type'],
                json_annotations.get('description', ''),
                json_annotations['layer'],
                json_annotations['startTimeUutc'],
                json_annotations['endTimeUutc'],
                portal_id=json_annotations['revId'],
                annotated_portal_ids=json_annotations['timeseriesRevIds']['timeseriesRevId'])]
        return annotations

    def add_annotations(self, annotations):
        """
        Adds a collection of Annotations to this dataset.
        """

        # request_body is oddly verbose because it was originally designed as XML.
        ts_revids = set()
        ts_annotations = []
        for a in annotations:
            if a.parent != self:
                raise ValueError(
                    'Annotation does not belong to this dataset. It belongs to dataset ' + a.parent.snap_id)
            annotated_revids = [detail.portal_id for detail in a.annotated]
            ts_annotation = {
                'timeseriesRevIds': {'timeseriesRevId': annotated_revids},
                'annotator': a.annotator,
                'type': a.type,
                'description': a.description,
                'layer': a.layer,
                'startTimeUutc': a.start_time_offset_usec,
                'endTimeUutc': a.end_time_offset_usec
            }
            if a.portal_id:
                ts_annotation['revId'] = a.portal_id
            ts_annotations.append(ts_annotation)
            ts_revids.update(annotated_revids)

        timeseries = [{'revId': ts_revid, 'label': self.ts_details_by_id[ts_revid].channel_label}
                      for ts_revid in ts_revids]
        request_body = {'timeseriesannotations': {
            'timeseries': {
                'timeseries': timeseries
            },
            'annotations': {
                'annotation': ts_annotations
            }
        }}
        req_path = '/services/timeseries/addAnnotationsToDataSnapshot/' + \
            self.snap_id
        http_method = 'POST'

        payload = self.session.create_ws_header(
            req_path, http_method, payload=json.dumps(request_body), request_json=True)
        url_str = self.session.url_builder(req_path)
        r = requests.post(url_str, headers=payload,
                          json=request_body, verify=False)
        if r.status_code != requests.codes.ok:
            response_body = r.text
            print(response_body)
            raise IeegConnectionError(
                'Could not add annotations')

    def move_annotation_layer(self, from_layer, to_layer):
        """
        Moves all annotations in layer from_layer to layer to_layer.

        :returns: the number of moved annotations.
        """

        req_path = '/services/timeseries/datasets/'\
            + self.snap_id\
            + '/tsAnnotations/'\
            + from_layer
        url_str = self.session.url_builder(req_path)

        query_params = {'toLayerName': to_layer}
        http_method = 'POST'
        headers = self.session.create_ws_header(
            req_path, http_method, query=query_params, request_json=True)

        response = requests.post(url_str, headers=headers, params=query_params, verify=False)
        if response.status_code != requests.codes.ok:
            response_body = response.text
            print(response_body)
            raise IeegConnectionError(
                'Could not move annotation layer ' + from_layer + ' to ' + to_layer)
        response_body = response.json()
        moved = response_body['tsAnnotationsMoved']['moved']
        return int(moved)

    def delete_annotation_layer(self, layer):
        """
        Deletes all annotations in the given layer.

        :returns: the number of deleted annotations.
        """

        req_path = '/services/timeseries/removeTsAnnotationsByLayer/'\
            + self.snap_id\
            + '/'\
            + layer
        url_str = self.session.url_builder(req_path)

        http_method = 'POST'
        headers = self.session.create_ws_header(
            req_path, http_method, request_json=True)

        response = requests.post(url_str, headers=headers, verify=False)
        if response.status_code != requests.codes.ok:
            response_body = response.text
            print(response_body)
            raise IeegConnectionError(
                'Could not delete annotation layer ' + layer)
        response_body = response.json()
        deleted = response_body['tsAnnotationsDeleted']['noDeleted']
        return int(deleted)

    @deprecated
    def getChannelLabels(self):
        return self.get_channel_labels()

    @deprecated
    def getData(self, start, duration, channels):
        return self.get_data(start, duration, channels)


class IeegConnectionError(Exception):
    """
    A simple exception for connectivity errors
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
