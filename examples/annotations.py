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
import functools
from pennprov.connection.mprov_connection import MProvConnection
from ieeg.auth import Session
from ieeg.dataset import Annotation
from ieeg.mprov_listener import MProvListener


def dataset_required(func):
    """
    Obtains dataset for func and calls it, passing dataset as first argument.
    """
    @functools.wraps(func)
    def pass_dataset(args):
        if not args.password:
            args.password = getpass.getpass('IEEG Password: ')
        if args.mprov_user and not args.mprov_password:
            args.mprov_password = getpass.getpass('MProv Password: ')
        if args.host:
            Session.host = args.host
            if args.port:
                Session.port = args.port
            Session.method = 'http' if args.no_ssl else 'https'
        mprov_listener = None
        if args.mprov_user:
            mprov_url = 'http://localhost:8088' if args.mprov_url is None else args.mprov_url
            if args.mprov_graph:
                MProvConnection.graph_name = args.mprov_graph
            mprov_connection = MProvConnection(
                args.mprov_user, args.mprov_password, mprov_url)
            mprov_listener = MProvListener(mprov_connection)
        with Session(args.user, args.password, mprov_listener=mprov_listener) as session:
            dataset = session.open_dataset(args.dataset)
            func(dataset, args)
            session.close_dataset(dataset)
    return pass_dataset


@dataset_required
def read(dataset, args):
    """
    Reads annotations from dataset.
    """
    layer_name = args.layer
    layer_to_count = dataset.get_annotation_layers()
    if not layer_name:
        print(layer_to_count)
    else:
        expected_count = layer_to_count.get(layer_name)
        if not expected_count:
            print('Layer', layer_name, 'does not exist')
            return
        actual_count = 0
        max_results = None if expected_count < 100 else 100
        call_number = 0
        while actual_count < expected_count:
            annotations = dataset.get_annotations(
                layer_name, first_result=actual_count, max_results=max_results)
            call_number += 1
            actual_count += len(annotations)
            first = annotations[0].start_time_offset_usec
            last = annotations[-1].end_time_offset_usec
            print("got", len(annotations), "annotations on call #",
                  call_number, "covering", first, "usec to", last, "usec")
        print("got", actual_count, "annotations in total")


@dataset_required
def add(dataset, args):
    """
    Adds two annotations to the given dataset layer.
    """
    layer_name = args.layer
    if not layer_name:
        layer_to_count = dataset.get_annotation_layers()
        print(layer_to_count)
    else:
        annotated_labels = [dataset.ch_labels[0], dataset.ch_labels[-1]]
        annotations = [Annotation(dataset, args.user,
                                  'Test', 'A test annotation', layer_name, 100000, 200100, annotated_labels=annotated_labels),
                       Annotation(dataset, args.user,
                                  'Test 2', 'A test annotation', layer_name, 200000, 300200, annotated_labels=annotated_labels)]

        dataset.add_annotations(annotations)
        layer_to_count = dataset.get_annotation_layers()
        print(layer_to_count)


@dataset_required
def move(dataset, args):
    """
    Move annotations from one layer to another.
    """
    from_layer = args.from_layer
    to_layer = args.to_layer
    layer_to_count = dataset.get_annotation_layers()
    if not from_layer:
        print(layer_to_count)
    else:
        count = layer_to_count.get(from_layer)
        if not count:
            print(from_layer, 'contains no annotations')
        else:
            print('Moving', count,
                  'annotations from', from_layer, 'to', to_layer)
            moved = dataset.move_annotation_layer(from_layer, to_layer)
            print('Moved', moved, 'annotations')
            print(dataset.get_annotation_layers())


@dataset_required
def delete(dataset, args):
    """
    Delete annotations from the given layer.
    """
    layer_to_count = dataset.get_annotation_layers()
    layer_name = args.layer
    if not layer_name:
        print(layer_to_count)
    else:
        print('Deleting', layer_to_count[layer_name],
              'annotations from', layer_name)
        deleted = dataset.delete_annotation_layer(layer_name)
        print('Deleted', deleted, 'annotations')
        print(dataset.get_annotation_layers())


def fail_no_command(args):
    """
    Reports failure when no subcommand was given.
    """
    args.parser.error('A subcommand is required.')


def validate(args):
    """
    Do any validation of args that argparse does not provide.
    """
    if hasattr(args, 'from_layer'):
        # Must be a move
        if (args.from_layer and not args.to_layer or args.to_layer and not args.from_layer):
            args.parser.error('Both from_layer and to_layer must be provided.')


def main():
    """
    Parses the command line and dispatches subcommand.
    """

    # create the top-level parser
    parser = argparse.ArgumentParser(
        epilog='<subcommand> -h for subcommand help')
    parser.add_argument('-u', '--user', required=True, help='username')
    parser.add_argument('-p', '--password',
                        help='password (will be prompted if missing)')
    parser.add_argument('--mprov_user', help='MProv username')
    parser.add_argument('--mprov_password',
                        help='MProv password (will be prompted if missing)')
    parser.add_argument('--mprov_url',
                        help='MProv URL')
    parser.add_argument('--mprov_graph',
                        help='MProv graph name')

    parser.add_argument('--host', help='the host')
    parser.add_argument('--no_ssl', action='store_true', default=False,
                        help="Do not use https. Ignored unless --host is set.")
    parser.add_argument(
        '--port', help='The port. Ignored unless --host is set.')
    parser.set_defaults(func=fail_no_command, parser=parser)

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands')

    dataset_parser = argparse.ArgumentParser(add_help=False)
    dataset_parser.add_argument('dataset', help='dataset name')

    layer_parser = argparse.ArgumentParser(add_help=False)
    layer_parser.add_argument(
        'layer', nargs='?', help='Layer name. If missing, print layers in dataset.')

    # The "read" command
    parser_read = subparsers.add_parser('read',
                                        parents=[dataset_parser, layer_parser],
                                        help='Read annotations from the given dataset layer.')
    parser_read.set_defaults(func=read, parser=parser_read)

    # The "add" command
    parser_add = subparsers.add_parser('add',
                                       parents=[dataset_parser, layer_parser],
                                       help='Add two test annotations to the given dataset layer.')
    parser_add.set_defaults(func=add, parser=parser_add)

    # The "delete" command
    parser_delete = subparsers.add_parser('delete',
                                          parents=[
                                              dataset_parser, layer_parser],
                                          help='Delete the  given annotation layer.')
    parser_delete.set_defaults(func=delete, parser=parser_delete)

    # The "move" command
    parser_move = subparsers.add_parser('move',
                                        parents=[dataset_parser],
                                        help="""Move annotations from the source layer
                                                to the destination layer.""")
    parser_move.add_argument(
        'from_layer', nargs='?', help='source layer')
    parser_move.add_argument(
        'to_layer', nargs='?', help='destination layer')
    parser_move.set_defaults(func=move, parser=parser_move)

    args = parser.parse_args()

    validate(args)
    args.func(args)


if __name__ == "__main__":
    main()
