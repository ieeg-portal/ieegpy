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
    if len(args) < 4:
        print(
            'Usage: delete_annotation_layer username password dataset_name [layer]')
        sys.exit(1)

    user = args[1]
    print('Logging into IEEG:', user, '/ ****')
    #Session.method = 'http://'
    #Session.host = '127.0.0.1'
    #Session.port = ':8888'
    session = Session(user, args[2])

    dataset = session.open_dataset(args[3])

    layer_name = args[4] if len(args) > 4 else None

    layer_to_count = dataset.get_annotation_layers()

    if not layer_name:
        print(layer_to_count)
    else:
        print('Deleting', layer_to_count[layer_name],
              'annotations from', layer_name)
        deleted = dataset.delete_annotation_layer(layer_name)
        print('Deleted', deleted, 'annotations')
        print(dataset.get_annotation_layers())
    session.close_dataset(dataset)
    session.close()


if __name__ == "__main__":
    main(sys.argv)
