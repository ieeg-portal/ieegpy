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
import numpy
from ieeg.auth import Session
from ieeg.processing import ProcessSlidingWindowPerChannel, ProcessSlidingWindowAcrossChannels


def main():
    """
    Print all montages for given dataset if no montage is specified.
    Or get small amount of montaged data if a montage is specified.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', required=True, help='username')
    parser.add_argument('-p', '--password',
                        help='password (will be prompted if omitted)')

    parser.add_argument('dataset', help='dataset name')
    parser.add_argument('montage', nargs='?', help='montage name')

    args = parser.parse_args()

    if not args.password:
        args.password = getpass.getpass()

    with Session(args.user, args.password) as session:
        dataset_name = args.dataset
        dataset = session.open_dataset(dataset_name)
        montages = dataset.montages
        if not args.montage:
            for name, montage_list in montages.items():
                for montage in montage_list:
                    print(name, montage.portal_id, montage.pairs)
        else:
            assert dataset.get_current_montage() is None
            # Requesting unrealisticly short durations to verify montage arithmetic.
            raw_data = dataset.get_data(
                0, 8000, list(range(len(dataset.ch_labels))))
            print('raw', raw_data)

            dataset.set_current_montage(args.montage)
            montage = dataset.get_current_montage()
            print(montage)
            montage_channels = [0]
            if len(montage.pairs) > 1:
                montage_channels.append(1)
            montaged_data = dataset.get_data(0, 4000, montage_channels)
            print('montaged 1', montaged_data)
            montaged_data = dataset.get_data(4000, 4000, montage_channels)
            print('montaged 2', montaged_data)

            montage_labels = [montage.pairs[i] for i in montage_channels]
            print(montage_labels)
            window_result = ProcessSlidingWindowPerChannel.execute(
                dataset, montage_labels, 0, 4000, 4000, 8000, numpy.mean)
            print('per channel', window_result)

            def row_mean(matrix):
                return numpy.mean(matrix, axis=1)

            window_result = ProcessSlidingWindowAcrossChannels.execute(
                dataset, montage_labels, 0, 4000, 4000, 8000, row_mean)
            print('across channels', window_result)
        session.close_dataset(dataset_name)


if __name__ == "__main__":
    main()
