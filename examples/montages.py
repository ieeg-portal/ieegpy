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
from ieeg.auth import Session


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
        print(montages)
        montage_names = [montage['@name'] for montage in montages]
        print(montage_names)
        data = dataset.get_data(0, 1000000, [0, 1, 2])
        print(data[0])
        session.close_dataset(dataset_name)


if __name__ == "__main__":
    main()
