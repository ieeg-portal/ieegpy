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

if len(sys.argv) < 4:
    print(
        'Usage: read_annotations username password dataset_name [layer_name]')
    sys.exit(1)

print('Logging into IEEG:', sys.argv[1], '/ ****')
session = Session(sys.argv[1], sys.argv[2])

dataset = session.open_dataset(sys.argv[3])

layer_name = sys.argv[4] if len(sys.argv) > 4 else None

layerToCount = dataset.get_annotation_layers()

if not layer_name:
    print(layerToCount)
else:
    expected_count = layerToCount[layer_name]
    actual_count = 0
    max_results = None if expected_count < 100 else 100
    call_count = 0
    while actual_count < expected_count:
        annotations = dataset.get_annotations(
            layer_name, first_result=actual_count, max_results=max_results)
        call_count += 1
        actual_count += len(annotations)
        first = annotations[0].start_time_offset_usec
        last = annotations[-1].end_time_offset_usec
        print("got ", len(annotations), " on call ",
              call_count, " covering ", first, " usec to ", last, " usec")
    print("got ", actual_count, " in total")

session.close_dataset(dataset)
