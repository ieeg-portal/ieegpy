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

from distutils.core import setup, Extension

setup(name='pyhabitat',
      version='1.0',
      description='API for the habitat platform (IEEG.org)',
      install_requires=['deprecation','requests','numpy','pandas'],
      py_modules=['hbt_auth', 'hbt_dataset'])
