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
from hbt_auth import Session


if len(sys.argv) < 2:
    print('Syntax: read_sample [user id (in double-quotes if it has a space)] [password]')
    sys.exit(1)

print ('Logging in', sys.argv[1], '/ ****')
s = Session(sys.argv[1], sys.argv[2])

# We pick one dataset...
ds = s.open_dataset('I004_A0003_D001')

print (ds)

s.close_dataset(ds)