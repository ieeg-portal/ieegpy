from ieeg.dataset import Dataset
import numpy as np
import math

class ProcessSlidingWindowPerChannel:
    """
    Access a sliding window over a subset of channels, do a single computation
    over each channel separately, and repeat for the duration

    Returns a 2D matrix
    """
    def execute(dataset, channel_list,
                start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_channel_computation):
        channel_indices = dataset.get_channel_indices(channel_list)

        # 0th window
        start_index = start_time_usec

        matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
        ret = np.reshape(np.array([per_channel_computation(channel) for channel in matrix.T]), (len(channel_indices), 1))

        for window in range(1, math.ceil(duration_usec / slide_usec)):
            start_index = start_time_usec + window * slide_usec

            matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
            x = np.reshape(np.array([per_channel_computation(channel) for channel in matrix.T]), (len(channel_indices),1))

            ret = np.hstack((ret, x))

        return ret

class ProcessSlidingWindowAcrossChannels:
    """
    Access a sliding window over a subset of channels, do a single computation
    over the 2D matrix, and repeat for the duration

    Returns an array
    """
    def execute(dataset, channel_subset_list, start_time_usec, window_size_usec, slide_usec, duration_usec,
                per_block_computation):
        channel_indices = dataset.get_channel_indices(channel_subset_list)

        ret = []

        for window in range(0, math.ceil(duration_usec / slide_usec)):
            start_index = start_time_usec + window * slide_usec

            matrix = dataset.get_data(start_index, window_size_usec, channel_indices)
            x = per_block_computation(matrix)

            ret = ret + [x]

        return np.array(ret)
