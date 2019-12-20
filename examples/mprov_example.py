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
import math as m
import numpy as np

from pennprov.connection.mprov_connection import MProvConnection
from ieeg.auth import Session
from ieeg.dataset import Annotation
from ieeg.ieeg_api import IeegServiceError
from ieeg.annotation_processing import SlidingWindowAnnotator


def negative_mean_annotator(window, annotation_layer):
    """
    Adds annotation if the mean of the samples is negative.
    """
    mean = np.mean(window.data_block)
    if mean >= 0 or m.isnan(mean):
        return None
    annotation_string = 'mean: ' + str(mean)
    start_offset_usec = window.window_start_usec
    end_offset_usec = start_offset_usec + window.window_size_usec
    annotation = Annotation(window.dataset,
                            'negative_mean_annotator',
                            annotation_string,
                            annotation_string,
                            annotation_layer,
                            start_offset_usec,
                            end_offset_usec,
                            annotated_labels=window.input_channel_labels)
    return annotation


def open_or_create_dataset(session, dataset_name, tool_name):
    """
    Either opens and returns dataset with the given name, or returns a copy
    of Study 005 with the given name and attributed to tool_name
    """
    try:
        dataset = session.open_dataset(dataset_name)
    except IeegServiceError as error:
        if not error.ieeg_error_code == 'NoSuchDataSnapshot':
            raise
        base_dataset_name = 'Study 005'
        base_dataset = session.open_dataset(base_dataset_name)
        # Copy dataset so that we have write access.
        dataset = base_dataset.derive_dataset(dataset_name, tool_name)
        print('Dataset {} does not exist. Created copy of {}'.format(
            dataset_name, base_dataset_name))
    return dataset


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
                        help="""A dataset to which you have write access.
                        If a dataset with this name does not exist a copy
                        of Study 005 is created and used.""")

    args = parser.parse_args()
    dataset_name = args.dataset_name

    if not args.password:
        args.password = getpass.getpass('IEEG Password: ')
    mprov_connection = None
    if args.mprov_user:
        mprov_password = args.mprov_password if args.mprov_password else getpass.getpass(
            'MProv Password: ')
        mprov_url = 'http://localhost:8088'
        MProvConnection.graph_name = dataset_name
        mprov_connection = MProvConnection(
            args.mprov_user, mprov_password, mprov_url)
    with Session(args.user, args.password) as session:
        tool_name = parser.prog
        dataset = open_or_create_dataset(session, dataset_name, tool_name)
        layer_name = 'negative mean layer ' + datetime.datetime.today().isoformat()
        dataset_duration_usec = dataset.end_time - dataset.start_time
        # Probably working with a copy of Study 005.
        # It has a gap at the beginning, so we'll try to skip it.
        study_005_post_gap_offset = 583000000
        start_time_usec = (
            study_005_post_gap_offset if dataset_duration_usec > study_005_post_gap_offset else 0)
        window_size_usec = 1000000
        slide_usec = 500000
        duration_usec = 120000000
        input_channel_labels = dataset.ch_labels[:2]
        window_annotator = SlidingWindowAnnotator(
            window_size_usec, slide_usec, negative_mean_annotator,
            mprov_connection=mprov_connection)
        print("Processing {} usec of dataset '{}' starting at {} usec with a {} usec slide."
              .format(duration_usec,
                      dataset.name,
                      start_time_usec,
                      slide_usec))
        if mprov_connection:
            print("Provenance graph '{}' will be viewable at {}/viz/.".format(
                mprov_connection.get_graph(),
                mprov_connection.configuration.host))
        annotations = window_annotator.annotate_dataset(dataset, layer_name,
                                                        start_time_usec=start_time_usec,
                                                        duration_usec=duration_usec,
                                                        input_channel_labels=input_channel_labels)
        print("Wrote {} annotations to layer '{}' in dataset '{}'.".format(
            len(annotations),
            layer_name,
            dataset.name))
        if mprov_connection:
            print("Wrote provenance of annotations to graph '{}'.".format(
                mprov_connection.get_graph()))
        session.close_dataset(dataset)


if __name__ == "__main__":
    main()
