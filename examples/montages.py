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

import argparse
import getpass
import numpy as np
from ieeg.auth import Session


def montage_to_matrix(dataset, montage):
    """
    Returns the matrix corresponding to the given montage.
    """
    pairs = montage['montagePairs']['montagePair']
    # If the montage only has one pair, pairs will be a dict instead of a list.
    if isinstance(pairs, dict):
        pairs = [pairs]
    matix_columns = []
    for pair in pairs:
        pair_channel = pair['@channel']
        # refChannel is optional
        pair_ref = pair.get('@refChannel')
        column = []
        for label in dataset.ch_labels:
            if label == pair_channel:
                column.append(1)
            elif label == pair_ref:
                column.append(-1)
            else:
                column.append(0)
        matix_columns.append(column)
    return np.column_stack(matix_columns)


def get_data(dataset, start, duration, montage_channels, montage_matrix):
    # remove columns that correspond to non-requested montage pairs.
    requested_matrix = montage_matrix[:, montage_channels]
    nonzero_channel_indices = requested_matrix.nonzero()[0]
    uniq_sorted_indices = list(set(nonzero_channel_indices))
    # remove rows of zeros (raw channels we are not using)
    reduced_matrix = requested_matrix[~np.all(requested_matrix == 0, axis=1), :]
    raw_data = dataset.get_data(start, duration, uniq_sorted_indices)
    montaged_data = np.matmul(raw_data, reduced_matrix)
    return montaged_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', required=True, help='username')
    parser.add_argument('-p', '--password',
                        help='password (will be prompted if missing)')

    parser.add_argument('dataset', help='dataset name')

    args = parser.parse_args()

    if not args.password:
        args.password = getpass.getpass()

    with Session(args.user, args.password) as session:
        dataset_name = args.dataset
        dataset = session.open_dataset(dataset_name)
        montages = dataset.get_montages()
        for montage in montages:
            matrix = montage_to_matrix(dataset, montage)
            if matrix.any():
                print(montage['@name'])
                print(matrix)
                montage_channels = [0]
                if matrix.shape[1] > 1:
                    montage_channels.append(1)
                raw_data = dataset.get_data(0, 6000, list(range(len(dataset.ch_labels))))
                print('raw', raw_data)
                montaged_data = get_data(dataset, 0, 6000, montage_channels, matrix)
                print('montaged', montaged_data)
        session.close_dataset(dataset_name)


if __name__ == "__main__":
    main()
