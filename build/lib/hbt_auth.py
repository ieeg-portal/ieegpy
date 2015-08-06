#!/usr/bin/python

import hashlib
import base64
import datetime
import requests
import xml.etree.ElementTree as ET
from hbt_dataset import Dataset as DS
    
class Session:
    """ Class representing Session on the platform """
    host = "localhost"
    #host = "view-qa.ieeg.org"
    port = ":8886"
    #port = ""
    method = 'http://'
    
    
    username = ""
    password = ""
    
    def __init__(self, name, pwd):
        self.username = name
        self.password = md5(pwd)    
        
    def urlBuilder(self, path):
        return Session.method + Session.host  + Session.port+ path
        
    def _createWSHeader(self,path, httpMethod, query, payload):
        dTime = datetime.datetime.now().isoformat()
        sig = self._signatureGenerator(path, httpMethod, query, payload, dTime)
        return {'username': self.username, 
            'timestamp': dTime, 
            'signature': sig, 
            'Content-Type': 'application/xml'};
        
    def _signatureGenerator(self, path, httpMethod, query, payload, dTime):
        """ Signature Generator, used to authenticate user in portal """
    
        m = hashlib.sha256()
        
        queryStr = ""
        if len(query):
            for k,v in query.items():
                queryStr += k + "=" + str(v) + "&"  
            queryStr = queryStr[0:-1]              
                    
        m.update(payload)        
        payloadHash =  base64.standard_b64encode(m.digest())
        
        toBeHashed = (self.username+"\n"+ 
            self.password+ "\n"+
            httpMethod+"\n"+
            self.host+"\n"+
            path+"\n"+
            queryStr+"\n"+
            dTime+"\n"+
            payloadHash)
    
        m2 = hashlib.sha256()
        m2.update(toBeHashed)
        return base64.standard_b64encode(m2.digest())
        
    def openDataset(self, name):
        """ Returning a dataset object """
        
        # Request location
        getIDbySnapNamePath = "/services/timeseries/getIdByDataSnapshotName/"
        
        # Create request content  
        httpMethod = "GET"
        reqPath = getIDbySnapNamePath + name
        payload = self._createWSHeader(reqPath, httpMethod, "", "")
        url = Session.method + Session.host  + Session.port+ reqPath
        
        r = requests.get(url, headers=payload,verify=False);
        
        # Check response
        print r.text
        if r.status_code != 200:
            raise connectionError('Cannot find Study,')
       
        # Request location 
        getTimeSeriesStr = '/services/timeseries/getDataSnapshotTimeSeriesDetails/'
        
        # Create request content 
        snapshotID = r.text
        reqPath = getTimeSeriesStr + snapshotID
        payload = self._createWSHeader(reqPath, httpMethod, "", "")
        url = Session.method + Session.host  + Session.port + reqPath
        r = requests.get(url, headers=payload,verify=False);
        
        # Return Habitat Dataset object
        return DS(ET.fromstring(r.text), snapshotID, self) 
        

def md5(userString):
    """ Returns MD5 hashed string """
    m = hashlib.md5()
    m.update(userString)
    return m.hexdigest()
    

class connectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
        
        
    