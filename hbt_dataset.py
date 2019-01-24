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

import xml.etree.ElementTree as ET
import datetime
import requests
import numpy as np
from deprecation import deprecated


class Dataset:
    """
    Class representing Dataset on the platform
    """
    snap_id = ""
    ch_labels = []  # Channel Labels
    ts_array = []   # Channel
    
    def __init__(self, ts_details, snapshot_id, parent):
        self.session = parent
        self.snap_id = snapshot_id
        details = ts_details.findall('details')[0]  #only one details in timeseriesdetails
        
        for dt in details.findall('detail'):
            name = dt.findall('name')[0].text
            self.ch_labels.append(name)
            self.ts_array.append(dt)
             
    def __repr__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."
             
    def __str__(self):
        return "Dataset with: " + str(len(self.ch_labels)) + " channels."
            
    def get_channel_labels(self):
        return self.ch_labels
             
    def get_data(self, start, duration, channels):
        """ Returns MEF data from Platform """
        
        def all_same(items):
            return all(x == items[0] for x in items)
        
        # Request location
        get_data_str = "/services/timeseries/getUnscaledTimeSeriesSetBinaryRaw/"

        # Build Data Content XML
        wrapper1 = ET.Element('timeSeriesIdAndDChecks')
        wrapper2 = ET.SubElement(wrapper1, 'timeSeriesIdAndDChecks')
        i=0
        for ts in self.ts_array:
            if i in channels:
                el1 = ET.SubElement(wrapper2, 'timeSeriesIdAndCheck')
                el2 = ET.SubElement(el1, 'dataCheck')
                el2.text = ts.findall('revisionId')[0].text
                el3 = ET.SubElement(el1, 'id')
                el3.text = ts.findall('revisionId')[0].text
            i += 1
            
        data = ET.tostring(wrapper1, encoding="us-ascii", method="xml")
        data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>' + data
        
        # Create request content        
        req_path = get_data_str + self.snap_id
        http_method = "POST"
        params = {'start': start, 'duration': duration}
        payload = self.session.create_ws_header(req_path, http_method, params, data)
        url_str = self.session.url_builder(req_path)
        
        # response to request 
        r = requests.post(url_str, headers=payload, params=params, data=data,verify=False)
        
        # collect data in numpy array
        d = np.fromstring(r.content, dtype='>i4') 
        h = r.headers
        
        # Check all channels are the same length
        sample_per_row = [int(numeric_string) for numeric_string in r.headers['samples-per-row'].split(',')]
        if not all_same(sample_per_row):
            raise IeegConnectionError('Not all channels in response have equal length')
            
        conv_f = np.array([float(numeric_string) for numeric_string in r.headers['voltage-conversion-factors-mv'].split(',')])
    
        #Reshape to 2D array and Multiply by conversionFactor
        d2 = np.reshape(d, (-1,len(sample_per_row))) * conv_f[np.newaxis,:]

        return d2
                
    @deprecated
    def getChannelLabels(self):
        return self.get_channel_labels()

    @deprecated
    def getData(self, start, duration, channels):
        return self.get_data(start, duration, channels)


class IeegConnectionError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
