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
import math
import numpy as np
from pennprov.metadata.stream_metadata import BasicTuple, BasicSchema

class Window:
    """
    A processing window over a Dataset.

    Attributes:
        dataset: The ieeg.dataset.Dataset of this window.
        input_channel_labels: The list of input channel labels of this window.
        data_block: The n x m array of sample values of this window.
                    The value of n is a function of the window_size_usec and the sample rate
                    of the input channels. The value of m is len(input_channel_labels)
        window_index: The index of this window in the stream of windows to which it belongs.
        window_start_usec: The microsecond offset into dataset of this window.
        window_size_usec: The length of this window in microseconds.
    """

    def __init__(self,
                 dataset,
                 input_channel_labels,
                 data_block,
                 window_index,
                 window_start_usec,
                 window_size_usec):
        self.dataset = dataset
        self.input_channel_labels = input_channel_labels
        self.data_block = data_block
        self.window_index = window_index
        self.window_start_usec = window_start_usec
        self.window_size_usec = window_size_usec

class ProcessSlidingWindowPerChannel:
    """
    Methods to process a sliding window per channel.
    """

    @staticmethod
    def write_window_annot(mprov_connection, input_name, input_start, input_duration,
                           output_name, output_index, output_value_json):
        basic_schema = BasicSchema(output_name, {'input': 'string',
                                                 'start': 'double',
                                                 'duration': 'double'})
        mprov_connection.store_windowed_result(output_name, output_index,
                                               BasicTuple(basic_schema,
                                                          {'input': input_name,
                                                           'start': input_start,
                                                           'duration': input_duration}),
                                               [input_start],
                                               output_name,
                                               input_start,
                                               input_start + input_duration)



    @staticmethod
    def execute(dataset, channel_list,
                start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_channel_computation):
        """
        Access a sliding window over a subset of channels, do a single computation
        over each channel separately, and repeat for the duration

        Returns a 2D matrix
        """
        return ProcessSlidingWindowPerChannel.execute_with_provenance(dataset, channel_list, start_time_usec, window_size_usec, slide_usec,
                                            duration_usec, per_channel_computation, None, None, None)

    @staticmethod
    def execute_with_provenance(dataset, channel_list,
                                start_time_usec, window_size_usec, slide_usec, duration_usec,
                                per_channel_computation, mprov_connection, op_name, in_name):
        channel_indices = dataset.get_channel_indices(channel_list)

        # 0th window
        start_index = start_time_usec

        matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
        ret = np.reshape(np.array([per_channel_computation(channel) for channel in matrix.T]),
                         (len(channel_indices), 1))

        if mprov_connection:
            ProcessSlidingWindowPerChannel.write_window_annot(mprov_connection, in_name, 0, window_size_usec,
                                    op_name, ret.shape[1] - 1, '')

        for window in range(1, int(math.ceil(duration_usec / slide_usec))):
            start_index = start_time_usec + window * slide_usec

            matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
            x = np.reshape(np.array([per_channel_computation(channel) for channel in matrix.T]),
                           (len(channel_indices), 1))

            ret = np.hstack((ret, x))

            if mprov_connection:
                ProcessSlidingWindowPerChannel.write_window_annot(mprov_connection, in_name, window, window_size_usec,
                                        op_name, ret.shape[1] - 1, '')

        return ret


class ProcessSlidingWindowAcrossChannels:
    """
    Methods to process a sliding window across channels.
    """


    @staticmethod
    def execute(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_block_computation):
        """
        Access a sliding window over a subset of channels, do a single computation
        over the 2D matrix, and repeat for the duration

        Returns an array
        """
        return ProcessSlidingWindowAcrossChannels.execute_with_provenance(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec,
                                            duration_usec,
                                            per_block_computation, None, None, None)

    @staticmethod
    def execute_with_provenance(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec,
                                duration_usec, per_block_computation, mprov_connection, op_name, in_name):
        channel_indices = dataset.get_channel_indices(channel_subset_list)
        ret = []

        for window in range(0, int(math.ceil(duration_usec / slide_usec))):
            start_index = start_time_usec + window * slide_usec

            matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
            x = per_block_computation(matrix)

            ret = ret + [x]
            if mprov_connection:
                ProcessSlidingWindowPerChannel.write_window_annot(mprov_connection, in_name, window, window_size_usec,
                                        op_name, len(ret) - 1, '')

        return np.array(ret)
