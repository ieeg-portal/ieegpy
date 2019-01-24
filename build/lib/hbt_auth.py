#!/usr/bin/python

import hashlib
import base64
import datetime
import requests
import xml.etree.ElementTree as ET
from hbt_dataset import Dataset as DS
from deprecation import deprecated


class Session:
    """
    Class representing Session on the platform
    """
    host = "www.ieeg.org"
    port = ""
    method = 'https://'

    username = ""
    password = ""
    
    def __init__(self, name, pwd):
        self.username = name
        self.password = md5(pwd)    

    @staticmethod
    def url_builder(self, path):
        return Session.method + Session.host + Session.port + path

    def create_ws_header(self, path, http_method, query, payload):
        d_time = datetime.datetime.now().isoformat()
        sig = self._signatureGenerator(path, http_method, query, payload, d_time)
        return {'username': self.username, \
            'timestamp': d_time, \
            'signature': sig, \
            'Content-Type': 'application/xml'};
        
    def _signature_generator(self, path, http_method, query, payload, d_time):
        """
        Signature Generator, used to authenticate user in portal
        """
    
        m = hashlib.sha256()
        
        query_str = ""
        if len(query):
            for k, v in query.items():
                query_str += k + "=" + str(v) + "&"
            query_str = query_str[0:-1]
                    
        m.update(payload)        
        payload_hash =  base64.standard_b64encode(m.digest())

        to_be_hashed = (self.username + "\n" +
                        self.password + "\n" +
                        http_method + "\n" +
                        self.host + "\n" +
                        path + "\n" +
                        query_str + "\n" +
                        dTime + "\n" +
                        payload_hash)
    
        m2 = hashlib.sha256()
        m2.update(to_be_hashed)
        return base64.standard_b64encode(m2.digest())
        
    def open_dataset(self, name):
        """
        Return a dataset object
        """
        
        # Request location
        get_id_by_snap_name_path = "/services/timeseries/getIdByDataSnapshotName/"
        
        # Create request content  
        http_method = "GET"
        req_path = get_id_by_snap_name_path + name
        payload = self._create_ws_header(req_path, http+method, "", "")
        url = Session.method + Session.host + Session.port + req_path
        
        r = requests.get(url, headers=payload,verify=False);
        
        # Check response
        print (r.text)
        if r.status_code != 200:
            raise connectionError('Cannot find Study,')
       
        # Request location 
        get_time_series_url = '/services/timeseries/getDataSnapshotTimeSeriesDetails/'
        
        # Create request content 
        snapshot_id = r.text
        req_path = get_time_series_url + snapshot_id
        payload = self._create_ws_header(req_path, http_method, "", "")
        url = Session.method + Session.host + Session.port + req_path
        r = requests.get(url, headers=payload, verify=False)
        
        # Return Habitat Dataset object
        return DS(ET.fromstring(r.text), snapshotID, self)

    def close_dataset(self, ds):
        """
        Close connection (for future use)
        :param ds: Dataset to close
        :return:
        """
        return

    # For backward-compatibility
    @deprecated
    @staticmethod
    def urlBuilder(self, path):
        return Session.url_builder(path)

    @deprecated
    def openDataset(self, name):
        return self.open_dataset(name)

    @deprecated
    def _createWSHeader(self, path, http_method, query, payload):
        return self.create_ws_header(path, http_method, query, payload)

def md5(user_string):
    """
    Return MD5 hashed string
    """
    m = hashlib.md5()
    m.update(user_string)
    return m.hexdigest()
    

