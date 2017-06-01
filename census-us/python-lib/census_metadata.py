# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dataiku.customrecipe import *

def get_metadata_sources(url):

    string_to_find = '.'
    metadata_d = {'name':[],'label':[],'concept':[],'type':[] }
    try:
        rv = requests.get(url,verify=False)
        status = 'ok'
    
    except:
        status = 'The US Census metadata page is not available: %s' % (url)
        print status
        
    if status == 'ok':
        datav = rv.text
        soupv = BeautifulSoup(datav, "html.parser")

        tablev = soupv.find('tbody')
        rowsv = tablev.find_all('tr')

        for row in rowsv:
            cols = row.find_all('td')
            #print cols
            concept = cols[2].get_text(strip=True)
            is_found = concept.find(string_to_find)
            ctype = concept[:is_found] 
            if is_found >0:
                metadata_d['type'].append( ctype )

            else :
                metadata_d['type'].append( 'NA' )

            metadata_d['name'].append(cols[0].get_text(strip=True) )
            metadata_d['label'].append( cols[1].get_text(strip=True) )
            metadata_d['concept'].append( cols[2].get_text(strip=True) )
    
    df_metadata_sources = pd.DataFrame(metadata_d)
    
    
    return status,df_metadata_sources

def build_metadata(df_metadata_source,var_list):
    df_vars = pd.DataFrame(var_list)
    df_vars.columns = ['name']
    
    dfv = pd.merge(df_metadata_source,df_vars, how='inner',left_on='name', right_on='name')
    
    return dfv