#!/usr/bin/python

##################################################################################
# Copyright 2013-19 by the Trustees of the University of Pennsylvania
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################

import sys
from ieeg.auth import Session


def main(args):
    """
    Prints the names of annotation layers on the given dataset if no layer_name is supplied. 
    Else fetches all annotations in the given layer.
    """
    if len(args) < 4:
        print(
            'Usage: read_annotations username password dataset_name [layer_name]')
        sys.exit(1)

    print('Logging into IEEG:', args[1], '/ ****')
    #Session.method = 'http://'
    #Session.host = '127.0.0.1'
    #Session.port = ':8888'
    session = Session(args[1], args[2])

    dataset = session.open_dataset(args[3])

    layer_name = args[4] if len(args) > 4 else None

    layer_to_count = dataset.get_annotation_layers()

    if not layer_name:
        print(layer_to_count)
    else:
        expected_count = layer_to_count[layer_name]
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

    session.close_dataset(dataset)
    session.close()


if __name__ == "__main__":
    main(sys.argv)
