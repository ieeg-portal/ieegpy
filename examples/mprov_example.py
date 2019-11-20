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
import random
from pennprov.connection.mprov_connection import MProvConnection
from ieeg.auth import Session
from ieeg.mprov_listener import MProvListener
from ieeg.dataset import Annotation, IeegConnectionError


def open_or_create_dataset(session, dataset_name, tool_name):
    """
    Either opens and returns dataset with the given name, or returns a copy
    of Study 005 with the given name and attributed to tool_name
    """
    try:
        dataset = session.open_dataset(dataset_name)
    except IeegConnectionError as error:
        # Not a foolproof check of non-existence vs something else,
        # but best we can do at the moment.
        if not error.value.startswith('Authorization'):
            raise error
        else:
            base_dataset = 'Study 005'
            dataset = session.open_dataset(base_dataset)
            # Copy dataset so that we have write access.
            dataset = dataset.derive_dataset(dataset_name, tool_name)
            print('Dataset {} does not exist. Created copy of {}'.format(
                dataset_name, base_dataset))
    return dataset


def create_annotatations(dataset, layer_name, tool_name):
    """
    Creates and returns test annotations for the given dataset.
    """
    annotations = []
    annotated_labels = dataset.ch_labels
    dataset_end_offset_usec = dataset.end_time - dataset.start_time
    annotation_length_usec = 5000000
    start_upper_bound_usec = dataset_end_offset_usec - annotation_length_usec
    start_lower_bound_usec = 0
    i = 0
    while i < 10 and start_lower_bound_usec + annotation_length_usec <= start_upper_bound_usec:
        start_offset_usec = random.randrange(
            start_lower_bound_usec, start_upper_bound_usec)
        end_offset_usec = start_offset_usec + annotation_length_usec
        annotation = Annotation(dataset,
                                tool_name,
                                'Test {}'.format(i),
                                'A test annotation',
                                layer_name,
                                start_offset_usec,
                                end_offset_usec,
                                annotated_labels=annotated_labels)
        annotations.append(annotation)
        start_lower_bound_usec = end_offset_usec
        i = i + 1
    return annotations


def annotate_dataset(dataset, annotations, tool_name):
    """
    Adds given annotations to the given dataset.
    """
    if annotations:
        annotations_dataset = dataset.name
        try:
            dataset.add_annotations(annotations)
        except IeegConnectionError as error:
            # Not a foolproof check of no-write-access vs something else,
            # but best we can do at the moment.
            if not error.value.startswith('Could not add annotations'):
                raise error
            else:
                annotations_dataset = dataset.name + ' copy'
                # Copy dataset so that we have write access.
                dataset_copy = dataset.derive_dataset(annotations_dataset, tool_name)
                dataset_copy.add_annotations(annotations)
        print('writing {} annotations to dataset {}'.format(
            len(annotations), annotations_dataset))


def main():
    """
    Parses the command line and dispatches subcommand.
    """

    # create the top-level parser
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', required=True, help='username')
    parser.add_argument('-p', '--password',
                        help='password (will be prompted if missing)')
    parser.add_argument('--mprov_user', help='MProv username')
    parser.add_argument('--mprov_password',
                        help='MProv password (will be prompted if missing)')

    parser.add_argument('dataset_name',
                        help='A dataset to which you have write access. If dataset does not exist a copy of an existing dataset is created.')

    args = parser.parse_args()
    dataset_name = args.dataset_name

    if not args.password:
        args.password = getpass.getpass('IEEG Password: ')
    if args.mprov_user and not args.mprov_password:
        args.mprov_password = getpass.getpass('MProv Password: ')
    mprov_listener = None
    if args.mprov_user:
        mprov_url = 'http://localhost:8088'
        MProvConnection.graph_name = dataset_name
        mprov_connection = MProvConnection(
            args.mprov_user, args.mprov_password, mprov_url)
        mprov_listener = MProvListener(mprov_connection)
    with Session(args.user, args.password, mprov_listener=mprov_listener) as session:
        tool_name = parser.prog
        dataset = open_or_create_dataset(session, dataset_name, tool_name)
        layer_name = tool_name + ' layer'
        annotations = create_annotatations(dataset, layer_name, tool_name)
        annotate_dataset(dataset, annotations, tool_name)
        session.close_dataset(dataset)


if __name__ == "__main__":
    main()
