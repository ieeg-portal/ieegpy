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
import datetime
from ieeg.auth import Session


def main():
    """
    Parses the command line and dispatches subcommand.
    """
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        epilog="""Arguments can also be placed in a text file, one per line.
                  Pass the file name prefixed by '@': %(prog)s @/path/to/arg_file.txt""")
    parser.add_argument('-u', '--user', required=True, help='username')
    parser.add_argument('-p', '--password',
                        help='password (will be prompted if missing)')

    parser.add_argument('datasets', nargs='+',
                        metavar='dataset', help='dataset name')

    args = parser.parse_args()

    if not args.password:
        args.password = getpass.getpass()

    with Session(args.user, args.password) as session:
        for dataset_name in args.datasets:
            dataset = session.open_dataset(dataset_name)
            start_time_uutc = min(
                [ts.start_time for ts in dataset.ts_details.values()])
            timestamp = start_time_uutc/1000000
            start_time = datetime.datetime.fromtimestamp(
                timestamp, datetime.timezone.utc)
            print('{0}, {1}'.format(dataset_name, start_time))
            session.close_dataset(dataset_name)


if __name__ == "__main__":
    main()
