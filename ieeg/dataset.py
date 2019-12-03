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
from collections import namedtuple
import numpy as np
import pandas as pd
from deprecation import deprecated
from ieeg.ieeg_api import IeegConnectionError


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

    def __init__(self, portal_id, name, label, duration, min_sample, max_sample, number_of_samples, start_time, end_time, sample_rate, voltage_conversion):
        self.portal_id = portal_id
        self.name = name
        self.channel_label = label
        self.duration = float(duration)
        self.min_sample = int(min_sample)
        self.max_sample = int(max_sample)
        self.number_of_samples = int(number_of_samples)
        self.start_time = int(start_time)
        self.end_time = int(end_time)
        self.sample_rate = float(sample_rate)
        self.voltage_conversion_factor = float(voltage_conversion)
        return

    def __str__(self):
        return ('{}({}) spans {} usec, range [{}-{}] in {} samples. ' +
                'Starts @{} uUTC, ends @{} uUTC ' +
                'with sample rate {} Hz and voltage conv factor {}').format(
                    self.name, self.channel_label, self.duration,
                    self.min_sample, self.max_sample, self.number_of_samples,
                    self.start_time, self.end_time, self.sample_rate,
                    self.voltage_conversion_factor)

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
        return "annotation({}): {}({},{})".format(self.portal_id, self.type, self.start_time_offset_usec, self.end_time_offset_usec)


HalfMontageChannel = namedtuple(
    'HalfMontageChannel', ['raw_label', 'raw_index'])


class Montage:
    """
    A montage.

    Attributes:
    parent: The dataset to which the montage applies.
    portal_id: The montage's id on ieeg.org.
    name: The montage's name. May not be unique.
    pairs: The list of channel label pairs in the montage
           in the form (channel label, optional reference channel label).
    matrix: A NumPy matrix representation of the montage with respect to the parent dataset.
            Shape is n x m where n is # of channels in the dataset and m is # of channel
            pairs in the Montage.
    """

    def __init__(self, dataset_parent, portal_id, name, json_pairs):
        self.parent = dataset_parent
        self.portal_id = portal_id
        self.name = name
        self.indexed_pairs = self._json_pairs_to_pairs(json_pairs)
        self.pairs = [(channel.raw_label, reference.raw_label if reference else None)
                      for channel, reference in self.indexed_pairs]
        self._matrix = self._calculate_matrix()
        # A cache mapping montage channel indices tuples to the info
        # returned by get_montage_info(...)
        self._montage_channels_to_info = {}

    def _label_to_half_montage_channel(self, raw_label):
        """
        Returns a HalfMontageChannel for the given raw dataset channel label.
        """
        try:
            chan_index = self.parent.ch_labels.index(raw_label)
            return HalfMontageChannel(raw_label, chan_index)
        except ValueError:
            return HalfMontageChannel(raw_label, None)

    def _json_pair_to_pair(self, json_pair):
        """
        Returns a (HalfMontageChannel, HalfMontageChannel) tuple given
        a {'@channel': string, optional '@refChannel': string} dict.
        """
        channel_half = self._label_to_half_montage_channel(
            json_pair['@channel'])
        # refChannel is optional
        reference_label = json_pair.get('@refChannel')
        reference_half = self._label_to_half_montage_channel(
            reference_label) if reference_label else None
        return (channel_half, reference_half)

    def _json_pairs_to_pairs(self, json_pairs):
        """
        Returns a list of (HalfMontageChannel, HalfMontageChannel) tuples derived
        from json_pairs

        :param json_pairs: a list of a {'@channel': string, optional '@refChannel': string}
                           dicts or a single such dict.
        """
        # If the montage only has one pair, pairs will be a dict instead of a list.
        if isinstance(json_pairs, dict):
            return [self._json_pair_to_pair(json_pairs)]
        return [self._json_pair_to_pair(json_pair) for json_pair in json_pairs]

    def _calculate_matrix(self):
        """
        Returns the matrix corresponding to this montage with respect parent dataset.
        """
        matrix_columns = []
        for pair_channel, pair_ref in self.indexed_pairs:
            column = [0] * len(self.parent.ch_labels)
            if pair_channel.raw_index is not None:
                column[pair_channel.raw_index] = 1
            if pair_ref and pair_ref.raw_index is not None:
                column[pair_ref.raw_index] = -1
            matrix_columns.append(column)
        return np.column_stack(matrix_columns)

    @classmethod
    def create_montage_map(cls, dataset, json_montages):
        """
        Returns map of Montage name to list of Montages with that name.
        Montages with the same name can be distinguished by Montage.portal_id.
        """
        montages = {}
        for json_montage in json_montages:
            montage = cls(dataset,
                          json_montage['@serverId'],
                          json_montage['@name'],
                          json_montage['montagePairs']['montagePair'])
            if montage.size() > 0:
                same_name = montages.get(montage.name)
                if same_name:
                    same_name.append(montage)
                else:
                    montages[montage.name] = [montage]
        return montages

    def size(self):
        """
        Returns the actual number of montage channels with respect to the parent dataset.
        May be smaller than the length of pairs if some pairs refer to labels that do
        not exist in the parent dataset.
        """
        return np.count_nonzero(np.count_nonzero(self._matrix, axis=0))

    def get_montage_info(self, montage_channels):
        """
        Returns a two-element tuple with the information necessary to compute the requested
        channels in this montage with data from ieeg.org.

        The first element is a list of the raw channel indices required to compute the montage
        for the requested montage_channels.

        The second element is a 2D array that will compute the montaged data when multuplied to
        the unmontaged data from ieeg.org.

        If the return value is (raw_channels, montage_matrix), then the shape of montage_matrix
        is len(raw_channels) rows by len(montage_channels) columns.

        :param montage_channels a list of indices into the list of pairs in this montage
        """
        key = tuple(montage_channels)
        cached_info = self._montage_channels_to_info.get(key)
        if cached_info:
            return cached_info
        # remove columns that correspond to non-requested montage pairs.
        requested_matrix = self._matrix[:, montage_channels]
        # figure out the raw channels we need to request in order to caclulate montage
        nonzero_channel_indices = requested_matrix.nonzero()[0]
        uniq_sorted_indices = list(set(nonzero_channel_indices))
        uniq_sorted_indices.sort()
        # remove rows of zeros (raw channels we are not using)
        reduced_matrix = requested_matrix[~np.all(
            requested_matrix == 0, axis=1), :]
        computed_info = (uniq_sorted_indices, reduced_matrix)
        self._montage_channels_to_info[key] = computed_info
        return computed_info

    def __repr__(self):
        return "montage(" + self.name + "): " + str(self.pairs)


class Dataset:
    """
    Class representing Dataset on the platform
    """

    _SERVER_GAP_VALUE = np.iinfo(np.int32).min

    def __init__(self, dataset_name, ts_details, snapshot_id, parent, json_montages=None):
        # type: (str, xml.etree.Element, str, ieeg.auth.Session) -> None
        self.snap_id = ""
        self.ts_details = {}  # Time series details by label
        self.ts_details_by_id = {}  # Time series details by portal_id
        self.ch_labels = []  # Channel Labels
        self.ts_array = []   # Channel
        self.name = dataset_name
        self.session = parent
        self.snap_id = snapshot_id
        # only one details in timeseriesdetails
        xml_details = ts_details.findall('details')[0]

        dataset_start_time = float('inf')
        dataset_end_time = -1
        for dt in xml_details.findall('detail'):
            # ET.dump(dt)
            name = dt.findall('channelLabel')[0].text
            portal_id = dt.findall('revisionId')[0].text
            start_time = int(dt.findall('startTime')[0].text)
            end_time = int(dt.findall('endTime')[0].text)
            details = TimeSeriesDetails(portal_id,
                                        dt.findall('name')[0].text,
                                        name,
                                        dt.findall('duration')[0].text,
                                        dt.findall('minSample')[0].text,
                                        dt.findall('maxSample')[0].text,
                                        dt.findall('numberOfSamples')[0].text,
                                        start_time,
                                        end_time,
                                        dt.findall('sampleRate')[0].text,
                                        dt.findall('voltageConversionFactor')[0].text,)
            self.ch_labels.append(name)
            self.ts_array.append(dt)
            self.ts_details[name] = details
            self.ts_details_by_id[portal_id] = details
            if start_time < dataset_start_time:
                dataset_start_time = start_time
            if end_time > dataset_end_time:
                dataset_end_time = end_time

        self.start_time = dataset_start_time
        self.end_time = dataset_end_time

        self.montages = Montage.create_montage_map(
            self, json_montages if json_montages else [])
        self.current_montage = None

    def __repr__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."

    def __str__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."

    def derive_dataset(self, derived_dataset_name, tool_name):
        """
        Returns a new Dataset which is a copy of this dataset and which has the given name.

        Create a copy of this dataset with the given name and attributed to the given tool.

        :param derived_dataset_name: The name of the new dataset
        :param tool_name: The name of the tool creating the dataset
        :return: A new Dataset which is a copy of this dataset and which has the given name.
        """
        self.session.api.derive_dataset(self,
                                        derived_dataset_name, tool_name)
        return self.session.open_dataset(derived_dataset_name)

    def get_channel_labels(self):
        return self.ch_labels

    def get_channel_indices(self, list_of_labels):
        """
        Create a list of indices (suitable for get_data) from a list
        of labels
        :param list_of_labels: Ordered list of channel labels (or two-element tuples of labels if current_montage is set)
        :return: Ordered list of channel indices
        """
        if self.current_montage is None:
            return [self.ch_labels.index(x) for x in list_of_labels]

        return [self.current_montage.pairs.index(x) for x in list_of_labels]

    def get_time_series_details(self, label):
        """
        Get the metadata on a channel

        :param label: Channel label
        :return: object of type TimeSeriesDetails
        """
        return self.ts_details[label]

    def set_current_montage(self, montage_name, portal_id=None):
        """
        Sets the current montage to the named montage.
        Use None to clear current montage.
        """
        if montage_name is None:
            self.current_montage = None
            return

        montages_with_name = self.montages[montage_name]
        if len(montages_with_name) == 1:
            self.current_montage = montages_with_name[0]
        else:
            new_montage = None
            for montage in montages_with_name:
                if montage.portal_id == portal_id:
                    new_montage = montage
                    break
            if new_montage:
                self.current_montage = new_montage
            else:
                raise ValueError('Montage '
                                 + montage_name
                                 + ', id '
                                 + portal_id
                                 + ' does not exist')

    def get_current_montage(self):
        """
        Returns the current montage
        """
        return self.current_montage

    def _get_unmontaged_data(self, start, duration, raw_channels):
        """
        Returns unmontaged data from the IEEG platform
        :param start: Start time (usec)
        :param duration: Number of usec to request samples from
        :param raw_channels: Integer indices of the channels we want
        :return: 2D array, rows = samples, columns = channels
        """

        def all_same(items):
            return all(x == items[0] for x in items)

        response = self.session.api.get_data(
            self, start, duration, raw_channels)
        # collect data in numpy array
        int_array = np.frombuffer(response.content, dtype='>i4')

        # Check all channels are the same length
        samples_per_row_array = [int(numeric_string)
                                 for numeric_string in response.headers['samples-per-row'].split(',')]
        if not all_same(samples_per_row_array):
            raise IeegConnectionError(
                'Not all channels in response have equal length')
        samples_per_row = samples_per_row_array[0]
        conv_f = np.array([float(numeric_string)
                           for numeric_string in response.headers['voltage-conversion-factors-mv'].split(',')])

        # Reshape to 2D array and Multiply by conversionFactor
        int_matrix = np.reshape(
            int_array, (samples_per_row, len(raw_channels)), order='F')
        unmontaged_data = int_matrix * conv_f[np.newaxis, :]
        unmontaged_data[int_matrix == Dataset._SERVER_GAP_VALUE] = np.nan

        return unmontaged_data

    def get_data(self, start, duration, channels):
        """
        Returns data from the IEEG platform using the current montage if any.
        :param start: Start time (usec)
        :param duration: Number of usec to request samples from
        :param channels: Integer indices of the channels we want.
                         If the current montage is set, the indices
                         are interpreted as montage channels.
        :return: 2D array, rows = samples, columns = channels
        """

        if not self.current_montage:
            return self._get_unmontaged_data(start, duration, channels)

        raw_channels, montage_matrix = self.current_montage.get_montage_info(
            channels)
        raw_data = self._get_unmontaged_data(start, duration, raw_channels)
        return np.matmul(raw_data, montage_matrix)

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

        # response to request
        response = self.session.api.get_annotation_layers(self)
        response_body = response.json()
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

    def get_annotations(self, layer_name,
                        start_offset_usecs=None, first_result=None, max_results=None):
        """
        Returns a list of annotations in the given layer ordered by start time.

        Given a Dataset ds with no new annotations being added, if ds.get_annotations('my_layer')
        returns 152 annotations, then ds.get_annotations('my_layer', max_results=100) will return
        the first 100 of those and ds.get_annotations('my_layer', first_result=100, max_results=100)
        will return the final 52.

        :param layer_name: The annotation layer to return
        :param start_offset_usec: If specified all returned annotations will have a
                                  start offset >= start_offset_usec
        :param first_result: If specified, the zero-based index of the first annotation to return.
        :param max_results: If specified, the maximum number of annotations to return.
        :returns: a list of annotations in the given layer ordered by start offset.
        """

        response = self.session.api.get_annotations(self, layer_name,
                                                    start_offset_usecs=start_offset_usecs,
                                                    first_result=first_result,
                                                    max_results=max_results)
        response_body = response.json()
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
        self.session.api.add_annotations(self, annotations)
        if self.session.mprov_listener:
            self.session.mprov_listener.on_add_annotations(annotations)

    def move_annotation_layer(self, from_layer, to_layer):
        """
        Moves all annotations in layer from_layer to layer to_layer.

        :returns: the number of moved annotations.
        """
        response = self.session.api.move_annotation_layer(
            self, from_layer, to_layer)
        response_body = response.json()
        moved = response_body['tsAnnotationsMoved']['moved']
        return int(moved)

    def delete_annotation_layer(self, layer):
        """
        Deletes all annotations in the given layer.

        :returns: the number of deleted annotations.
        """
        response = self.session.api.delete_annotation_layer(self, layer)
        response_body = response.json()
        deleted = response_body['tsAnnotationsDeleted']['noDeleted']
        return int(deleted)

    @deprecated
    def getChannelLabels(self):
        return self.get_channel_labels()

    @deprecated
    def getData(self, start, duration, channels):
        return self.get_data(start, duration, channels)
