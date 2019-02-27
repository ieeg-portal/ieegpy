from ieeg.dataset import Dataset
import numpy as np
import math
import pennprov.connection.mprov_connection
from pennprov.metadata.stream_metadata import BasicTuple, BasicSchema


class ProcessSlidingWindowPerChannel:
    """
    Access a sliding window over a subset of channels, do a single computation
    over each channel separately, and repeat for the duration

    Returns a 2D matrix
    """

    def write_window_annot(mprov_connection, input_name, input_start, input_duration,
                           output_name, output_index, output_value_json):
        # mprov_connection.store_windowed_result(output_name, output_index,
        #                                        BasicTuple(BasicSchema(output_name, {'input': 'string',
        #                                                                             'start': 'double',
        #                                                                             'duration': 'double'}),
        #                                                 {'input': input_name,
        #                                                    'start': input_start,
        #                                                    'duration': input_duration}),
        #                                                     [input_start],
        #                                                    output_name,
        #                                                    input_start,
        #                                                    input_start + input_duration)
        return

    def execute(dataset, channel_list,
                start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_channel_computation):
        return ProcessSlidingWindowPerChannel.execute_with_provenance(dataset, channel_list, start_time_usec, window_size_usec, slide_usec,
                                            duration_usec, per_channel_computation, None, None, None)

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

        for window in range(1, math.ceil(duration_usec / slide_usec)):
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
    Access a sliding window over a subset of channels, do a single computation
    over the 2D matrix, and repeat for the duration

    Returns an array
    """

    def execute(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_block_computation):
        return ProcessSlidingWindowAcrossChannels.execute_with_provenance(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec,
                                            duration_usec,
                                            per_block_computation, None, None, None)

    def execute_with_provenance(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec,
                                duration_usec, per_block_computation, mprov_connection, op_name, in_name):
        channel_indices = dataset.get_channel_indices(channel_subset_list)

        ret = []

        for window in range(0, math.ceil(duration_usec / slide_usec)):
            start_index = start_time_usec + window * slide_usec

            matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
            x = per_block_computation(matrix)

            ret = ret + [x]
            if mprov_connection:
                ProcessSlidingWindowPerChannel.write_window_annot(mprov_connection, in_name, window, window_size_usec,
                                        op_name, len(ret) - 1, '')

        return np.array(ret)
