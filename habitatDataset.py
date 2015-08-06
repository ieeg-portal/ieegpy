#!/usr/bin/python
import xml.etree.ElementTree as ET
import datetime
import requests
import numpy as np


class Dataset:
    """ Class representing Dataset on the platform """
    snapID = ""
    chLabels = []  # Channel Labels
    tsArray = []   # Channel 
    
    def __init__(self, tsDetails, snapshotID, parent):
        self.session = parent
        self.snapID = snapshotID
        details = tsDetails.findall('details')[0]  #only one details in timeseriesdetails
        
        for dt in details.findall('detail'):
            name = dt.findall('name')[0].text
            self.tsd.append(name)
            self.tsArray.append(dt)
             
             
    def __repr__(self):
        return "Dataset with: " + str(len(self.chLabels)) + " channels."
             
    def __str__(self):
        return "Dataset with: " + str(len(self.chLabels)) + " channels."
            
    def getChannelLabels(self):
        return self.chLabels
             
    def getData(self, start, duration, channels):
        """ Returns MEF data from Platform """
        
        def all_same(items):
            return all(x == items[0] for x in items)
        
        # Request location
        getDataStr = "/services/timeseries/getUnscaledTimeSeriesSetBinaryRaw/"

        # Build Data Content XML
        wrapper1 = ET.Element('timeSeriesIdAndDChecks')
        wrapper2 = ET.SubElement(wrapper1, 'timeSeriesIdAndDChecks')
        i=0
        for ts in self.tsArray:
            if(i in channels):
                el1 = ET.SubElement(wrapper2, 'timeSeriesIdAndCheck')
                el2 = ET.SubElement(el1, 'dataCheck')
                el2.text = ts.findall('revisionId')[0].text
                el3 = ET.SubElement(el1, 'id')
                el3.text = ts.findall('revisionId')[0].text
            i += 1
            
        data = ET.tostring(wrapper1, encoding="us-ascii", method="xml")
        data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>' + data
        
        # Create request content        
        reqPath = getDataStr + self.snapID
        httpMethod = "POST"
        params = {'start': start, 'duration': duration}
        payload = self.session._createWSHeader(reqPath, httpMethod, params, data)
        urlStr = self.session.urlBuilder(reqPath)
        
        # response to request 
        r = requests.post(urlStr, headers=payload, params=params,data=data,verify=False)
        
        # collect data in numpy array
        d = np.fromstring(r.content, dtype='>i4') 
        h = r.headers
        
        # Check all channels are the same length
        samplePerRow = [int(numeric_string) for numeric_string in r.headers['samples-per-row'].split(',')]
        if not all_same(samplePerRow):
            raise connectionError('Not all channels in response have equal length')
            
        convF = np.array([float(numeric_string) for numeric_string in r.headers['voltage-conversion-factors-mv'].split(',')])
    
        #Reshape to 2D array and Multiply by conversionFactor
        d2 = np.reshape(d, (-1,len(samplePerRow))) *  convF[np.newaxis,:]

        return d2
                
        