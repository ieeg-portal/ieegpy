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
import numpy as np

from pennprov.connection.mprov_connection import MProvConnection
from ieeg.auth import Session
from ieeg.mprov_listener import MProvListener
from ieeg.dataset import Annotation
from ieeg.ieeg_api import IeegServiceError
from ieeg.processing import ProcessSlidingWindowAcrossChannels


class NegativeMeanAnnotator:
    """
    Silly annotator to work with ProcessSlidingWindowAcrossChannels.execute_with_provenance()
    to annotate windows with negative means.
    Provides context to the computation over the window.
    """

    def __init__(self, dataset, channel_labels, layer_name,
                 start_time_usec, window_size_usec, slide_usec):
        self.annotations = []
        self.dataset = dataset
        self.channel_labels = channel_labels
        self.layer_name = layer_name

        self.current_block_index = 0
        self.start_time_usec = start_time_usec
        self.window_size_usec = window_size_usec
        self.slide_usec = slide_usec
        self.annotator_name = __class__.__name__

    def process_data_block(self, data_block):
        """
        Adds annotation to self.annotations if the mean of data_block is negative.
        """
        mean = np.mean(data_block)
        if mean < 0:
            annotation_string = 'mean: ' + str(mean)
            start_offset_usec = (self.start_time_usec +
                                 self.current_block_index * self.slide_usec)
            end_offset_usec = start_offset_usec + self.window_size_usec
            annotation = Annotation(self.dataset,
                                    self.annotator_name,
                                    annotation_string,
                                    annotation_string,
                                    self.layer_name,
                                    start_offset_usec,
                                    end_offset_usec,
                                    annotated_labels=self.channel_labels)
            self.annotations.append(annotation)
        self.current_block_index += 1


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
    if args.mprov_user and not args.mprov_password:
        args.mprov_password = getpass.getpass('MProv Password: ')
    mprov_connection = None
    mprov_listener = None
    if args.mprov_user:
        mprov_url = 'http://localhost:8088'
        MProvConnection.graph_name = dataset_name
        mprov_connection = MProvConnection(
            args.mprov_user, args.mprov_password, mprov_url)
        mprov_listener = None # MProvListener(mprov_connection)
    with Session(args.user, args.password, mprov_listener=mprov_listener) as session:
        tool_name = parser.prog
        dataset = open_or_create_dataset(session, dataset_name, tool_name)
        layer_name = 'negative mean layer ' + datetime.datetime.today().isoformat()
        labels = dataset.get_channel_labels()
        dataset_duration_usec = dataset.end_time - dataset.start_time
        # Probably working with a copy of Study 005.
        # It has a gap at the beginning, so we'll try to skip it.
        study_005_post_gap_offset = 583000000
        start_time_usec = (
            study_005_post_gap_offset if dataset_duration_usec > study_005_post_gap_offset else 0)
        window_size_usec = 1000000
        slide_usec = 500000
        duration_usec = 60000000
        annotator = NegativeMeanAnnotator(
            dataset, labels, layer_name, start_time_usec, window_size_usec, slide_usec)
        ProcessSlidingWindowAcrossChannels.execute_with_provenance(
            annotator.dataset,
            annotator.channel_labels,
            annotator.start_time_usec,
            annotator.window_size_usec,
            annotator.slide_usec,
            duration_usec,
            annotator.process_data_block,
            mprov_connection,
            annotator.annotator_name,
            annotator.dataset.name)
        dataset.add_annotations(annotator.annotations)
        print("wrote {} annotations to layer '{}' in dataset '{}'".format(
            len(annotator.annotations),
            annotator.layer_name,
            dataset.name))
        if args.mprov_user:
            print("wrote provenance of annotations to graph '{}'".format(
                MProvConnection.graph_name))
        session.close_dataset(dataset)


if __name__ == "__main__":
    main()
