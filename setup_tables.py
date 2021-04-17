# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 17:14:49 2021

@author: phili
"""
column_list = ['requestUnixTime', 'epochTime', 'seconds', 'minutes', 'isDeparture', 'affectedByLayover', 'branch', 'dirTag', 'vehicle', 'block', 'tripTag']
realtime_data = 
# Define a list of stops for 52F Westbound: 
Stoplist_52F_W = [
      '6154',
      '5411',
      '5411',
      '5368',
      '5386',
      '7038',
      '7038',
      '5411',
      '7038',
      '7038',
      '7038',
      '6154',
      '14733',
      '5352',
      '7038',
      '5352',
      '7038',
      '6154',
      '7038',
      '7038',
      '7038',
      '7040',
      '5352',
      '6154',
      '5352',
      '5411',
      '5411',
      '7038',
      '7038',
      '7038',
      '5411',
      '5352',
      '5411',
      '7038',
      '5352',
      '7038',
      '7038',
      '14725',
      '14725'
] 

# Define a list of stops for 52F Eastbound : 
Stoplist_52F_E = [
          '14725',
          '7038',
          '7040',
          '4750',
          '7038',
          '7038',
          '7038',
          '7038',
          '7038',
          '7038',
          '5411',
          '7038',
          '7038',
          '7038',
          '5386',
          '7040',
          '7038',
          '7038',
          '7038',
          '7038',
          '7038',
          '5352',
          '7038',
          '5352',
          '7038',
          '14725',
          '7038',
          '4750',
          '7038',
          '7038',
          '6154',
          '7038',
          '5352',
          '7038',
          '5352',
          '6154',
          '5411',
          '7038',
          '5352',
          '7038',
          '7038',
          '5386'
]

import requests, time
import xml.etree.ElementTree as ET

def process( api_call ): 
    r = requests.get( api_call ) 
    t = time.time() 
    root = ET.fromstring(r.content)
    
    for child in root.iter('prediction'):
        # ==== PULL THE DATA FOR THIS STOP REQUEST ==== # 
        a = child.attrib
        datalist = [[
                t, 
                a["epochTime"],
                a["seconds"],
                a["minutes"],
                a["isDeparture"],
                a["affectedByLayover"],
                a["branch"],
                a["dirTag"],
                a["vehicle"],
                a["block"],
                a["tripTag"]
        ]]
        
        # ==== LOG THE DATA IN THE REALTIME DATAFRAME ==== # 
        call_data = pd.DataFrame( data = datalist, columns = data_headers ) 
        realtime_data = realtime_data.append( call_data ) 
    
    
