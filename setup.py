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

import setuptools
from distutils.core import setup, Extension

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='ieeg',
      version='1.6',
      description='API for the IEEG.org platform',
      install_requires=['deprecation','requests','numpy','pandas', 'pennprov==2.2.4'],
      packages=setuptools.find_packages(),
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/ieeg-portal/ieegpy",
      classifiers=[
          'Programming Language :: Python :: 2-3',
          'License :: OSI Approved :: Apache License',
          'Operating System :: OS Independent',
      ])
