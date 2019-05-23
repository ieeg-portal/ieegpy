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
        'Usage: read_annotations username password dataset_name')
    sys.exit(1)

print('Logging into IEEG:', sys.argv[1], '/ ****')
session = Session(sys.argv[1], sys.argv[2])

# We pick one dataset...
dataset = session.open_dataset(sys.argv[3])

layerToCount = dataset.list_annotation_layers()
print(layerToCount)
layers = [key for key, value in layerToCount.items() if value < 50]
print(layers)

for layer in layers:
    annotations = dataset.get_annotations(layer)
    print(annotations)
    print(len(annotations))

session.close_dataset(dataset)
