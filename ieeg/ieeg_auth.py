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
import hashlib
import base64
import datetime
import requests
from requests.auth import AuthBase
import urllib3


class IeegAuth(AuthBase):
    """Attaches IEEG authentication headers to the given Request object."""

    def __init__(self, username, password):
        self.username = username
        self.password = self._md5(password)

    def __call__(self, r):
        d_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        signature = self._signature_generator(r, d_time)
        r.headers['username'] = self.username
        r.headers['timestamp'] = d_time
        r.headers['signature'] = signature
        return r

    def _signature_generator(self, prepared_request, d_time):
        """
        Signature Generator, used to authenticate user in portal
        """
        parsed_url = urllib3.util.parse_url(prepared_request.url)
        host = parsed_url.host
        path = requests.compat.unquote(parsed_url.path)
        query = parsed_url.query or ''

        payload = b''
        if prepared_request.body and isinstance(prepared_request.body, str):
            payload = prepared_request.body.encode('utf-8')
        elif prepared_request.body:
            payload = prepared_request.body
        payload_hasher = hashlib.sha256()
        payload_hasher.update(payload)
        payload_hash = base64.standard_b64encode(payload_hasher.digest())

        to_be_hashed = (self.username + "\n" +
                        self.password + "\n" +
                        prepared_request.method + "\n" +
                        host + "\n" +
                        path + "\n" +
                        query + "\n" +
                        d_time + "\n" +
                        payload_hash.decode('utf-8'))
        # print('In IeegAuth\n{!s}'.format(to_be_hashed))
        sig_hasher = hashlib.sha256()
        sig_hasher.update(to_be_hashed.encode('utf-8'))
        return base64.standard_b64encode(sig_hasher.digest())

    def _md5(self, user_string):
        """
        Return MD5 hashed string
        """
        hasher = hashlib.md5()
        hasher.update(user_string.encode('utf-8'))
        return hasher.hexdigest()
