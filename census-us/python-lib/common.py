# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
import datetime
import census_resources
import json
import requests
import zipfile
import os
import shutil




def get_date():
    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    dt  = datetime.datetime.now().strftime(dt_format)
    return dt

def recipe_config_get_str_or_none(param_name):
    v = get_recipe_config().get(param_name, None)
    if v is not None and len(v) == 0:
        v =None
    return v


def get_state_structure(P_STATES_TYPE_NAME):
    dict_ref =  census_resources.get_dict_ref()
    dict_structure = dict()
    for i in range(0, len(dict_ref['state_2digits'])):
        name = dict_ref['state_fullname_w1'][i]
        twodigits = dict_ref['state_2digits'][i]
        twoletters = dict_ref['state_2letters'][i]
        
        structure = {u'attributes':
                       {u'state_2digits': twodigits
                        ,u'state_2letters': twoletters
                        ,u'state_fullname_w1': name
                        
                        }}
        
        if P_STATES_TYPE_NAME =='state_2digits':
            dict_structure[twodigits]=structure
        elif P_STATES_TYPE_NAME =='state_fullname_w1':
            dict_structure[name]=structure
        elif P_STATES_TYPE_NAME =='state_2letters':
            dict_structure[twoletters]=structure

    return dict_structure



def state_to_2letters_format(P_STATES_TYPE_NAME, state_list_):
    
    dict_states_ = get_state_structure(P_STATES_TYPE_NAME)
    dict_states = get_state_structure('state_2letters')
    
    state_list = [s for s in state_list_ if s in dict_states_.keys()]
    state_list_rejected = [sr for sr in state_list_ if sr not in dict_states_.keys()]
    
    if P_STATES_TYPE_NAME is not 'state_2letters':        
        state_list_corresp = [dict_states_[s1][u'attributes'][u'state_2letters'] for s1 in state_list]
        
    else:
        state_list_corresp = state_list
        
    return state_list_corresp,state_list_rejected,dict_states


def create_folder(path_,FOLDER_NAME,replace): #=True
    p_ = os.path.join(path_ + FOLDER_NAME)
    
    if not os.path.exists(p_):
        os.makedirs(p_)
        print '> Created folder: %s' % (p_)
    
    else:
        if replace is True:
            cmd = "rm -rf %s" % (p_)
            os.system(cmd)
            os.makedirs(p_)
            print '> Replaced folder: %s' % (p_)
                        
        else:
            print '> Will re use folder: %s' % (p_)
            
  
            
def us_census_source_collector(P_USE_PREVIOUS_SOURCES,P_CENSUS_TYPE,P_CENSUS_CONTENT,P_CENSUS_LEVEL,path_datadir_tmp,FOLDER_NAME,state_list,dict_states):
    
    
    
    ##### collect the fields definitions
    fields_definition_url_file = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['url']
    fields_definition_url_file_template = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['geo_header_template_url']
    geo_header_file = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['geo_header_filename']
    
    lim = fields_definition_url_file[::-1].find('/')
        
    if P_USE_PREVIOUS_SOURCES is False or os.path.exists(path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file[-lim:]) is False:
    
        response = requests.get(fields_definition_url_file)
        with open( os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file[-lim:]) , 'wb') as f:

            f.write(response.content)
        print '[+] downloaded: %s' % fields_definition_url_file

    base_fdir = path_datadir_tmp + FOLDER_NAME
    fdef_dir = path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file[-lim:][:-4]
    
    
    create_folder(base_fdir + '/',fields_definition_url_file[-lim:][:-4],True)
        
    with zipfile.ZipFile(os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file[-lim:])  , 'r') as zf:
        zf.extractall( fdef_dir )
        print 'Fields definition extracted'

    
    ## For taking into account that some vintage like ACS52013 does not have a structure with the template and a folder
    ## If no template, recreate the same structure as the alternative one.
    if fields_definition_url_file != fields_definition_url_file_template:
        
        lim_t = fields_definition_url_file_template[::-1].find('/')
        
        response = requests.get(fields_definition_url_file_template)
        with open( os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file_template[-lim_t:]) , 'wb') as f:
            f.write(response.content)
        print '[+] downloaded: %s' % fields_definition_url_file_template
        
        
        fdef_t_dir = path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file_template[-lim_t:][:-4]
    
        create_folder(base_fdir + '/',fields_definition_url_file_template[-lim_t:][:-4],True)

        with zipfile.ZipFile(os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + fields_definition_url_file_template[-lim_t:])  , 'r') as zf:
            zf.extractall( fdef_t_dir )
        
        for fs in os.listdir(fdef_t_dir):
            if fs.endswith('.xls'):
                template_definition_xls_file = fs

                
        segment_folder_name = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['folder_name']
        
        if segment_folder_name == '':
            segment_folder_name_alternative = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['geo_header_template_folder_name']

            create_folder(fdef_dir+'/',segment_folder_name_alternative,True)
            for fs in os.listdir(fdef_dir):
                if fs.endswith('.xls'):
                    shutil.move(fdef_dir +'/' + fs, fdef_dir+'/'+segment_folder_name_alternative)

            shutil.move(fdef_t_dir +'/' + template_definition_xls_file, fdef_dir+'/')

    

    ### download the ZCTA to ZIPCODE file
    if P_CENSUS_TYPE=='ZCTA':
        crosswalk_zcta_url_file = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['ZCTA_TO_ZIPCODE']['url']
        lim = crosswalk_zcta_url_file[::-1].find('/')
        
        if not os.path.exists(path_datadir_tmp + FOLDER_NAME+ '/' + crosswalk_zcta_url_file[-lim:] ):
            response = requests.get(crosswalk_zcta_url_file)
            with open(path_datadir_tmp + FOLDER_NAME+ '/' + crosswalk_zcta_url_file[-lim:], 'wb') as fc:

                fc.write(response.content)
            print '[+] ZCTA to ZIPCODE crosswalk downloaded'
        
    
    
    #### collect the geo referential from US Census API for finding the Level.
    print 'Calling Census API...'
    api_url = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['levels_code']
    
    req = requests.get(api_url) 
    data_d = req.json()
    geo_ref = pd.DataFrame(data_d['fips'])
    
        

    ####### collect the segment files.
    for state in state_list:
        ustate = state.upper()

        print 'Doing state: %s' % state
        

        state_name = dict_states[state]['attributes']['state_fullname_w1']
        state_number = dict_states[state]['attributes']['state_2digits']
    
        
        state_dir = path_datadir_tmp + FOLDER_NAME+'/'+ state 
        
        create_folder(base_fdir +'/',state,True)
            
        dict_pattern_files = census_resources.dict_pattern_files_[P_CENSUS_TYPE]

        if P_CENSUS_LEVEL in ('TRACT','BLOCK_GROUP'):
            ziptocollect = dict_pattern_files['v1']['TB']
            state_dir_level = state_dir +'/'+ 'TRACT_BG_SEG'


        else:
            ziptocollect = dict_pattern_files['v1']['OT']
            state_dir_level = state_dir +'/'+ 'NO_TRACT_BG_SEG'


        if os.path.exists(state_dir_level):
            cmd = "rm -rf %s" % (state_dir_level)
            os.system(cmd)
            os.mkdir(state_dir_level)
        else:
            os.mkdir(state_dir_level)


        filename = state_name + '_' + ziptocollect + '.zip'
                
        if P_USE_PREVIOUS_SOURCES is False or os.path.exists(path_datadir_tmp + FOLDER_NAME+ '/' + filename) is False:
            data_url = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['data_by_state'] + filename

            print '[+] downloading: %s' % filename
            print '--> from this url: %s' % (data_url)

            try:
                response = requests.get( data_url )
                with open( os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + filename) , 'wb') as f:
                    f.write(response.content)
                print '[+] downloaded: %s' % filename
            except:
                print 'Process failed to download: %s' % filename
                print 'Check if the Census server is available'
   
        with zipfile.ZipFile(os.path.join(path_datadir_tmp + FOLDER_NAME+ '/' + filename)  , 'r') as zf:
            zf.extractall( state_dir_level )
            print 'All segment files extracted from %s' % (filename)



    ### header for master segment file and level
    
    dico_sumlevel = {}
    for i in xrange(1,len(data_d[u'fips'])):
        dico_sumlevel[data_d['fips'][i]['name']] =  data_d['fips'][i]['geoLevelId']

    level_api_name = census_resources.dict_level_corresp['v1'][P_CENSUS_LEVEL]['census_name']
    sumlevel_val = [int(dico_sumlevel[level_api_name])]  #EXAMPLE : sumlevel_val=[150]
    
    
    
    return sumlevel_val,fdef_dir,geo_header_file,dict_pattern_files #,status



def rep(value):
    missing = ['NULL','','null','Null']
    if value == '.' or value in missing:
        return np.nan
    else:
        return value

    
def df_dropna(df,data_columns):
    for c in df.columns:
        if c in data_columns:
            if df[c].dtype == 'object':
                df[c] = df[c].map(rep)
    df = df.dropna()
    
    return df
        
    
def impute_strategy(strategy,threshold_,df,data_columns,let_below_threshold):
    N = float(df.shape[0])
    if strategy != 'dropna':
        for c in df.columns:
            if c in data_columns:
                    
                if df[c].dtype == 'object' or df[c].dtype =='str':
                    df[c] = df[c].map(rep)
                    df[c] = df[c].astype('float64')
    
                pct_full = df[c].dropna().count() / N  
                if pct_full >= threshold_ and pct_full < 1:
                    if strategy=='median':
                        v = df[c].dropna().median()
                        
                    if strategy=='mode':
                        v = df[c].dropna().mode()
                        
                    if strategy=='average':
                        v = df[c].dropna().mean()  
                        
                    df[c] = df[c].fillna(v) 
                                     
                if let_below_threshold is False:
                    if pct_full < threshold_:
                        del df[c]
                            
    if strategy== 'dropna':
        df = df_dropna(df,data_columns)
               
    return df
        


def rescaling(df,data_columns):
    for c in df.columns:
        if c in data_columns:
            m = df[c].mean()
            std = df[c].std()
            if std == 0.:
                del df[c]
            else:
                df[c]=(df[c] - m).astype(np.float64) / std
                #df[c] = df[c].replace([np.inf, -np.inf], np.nan)
    return df


def volumes_tallies_printer(df,P_GENERATE_ALL_THE_CENSUS_LEVEL,P_CENSUS_LEVEL):
    print '-------------- volumes check------------------'
    print 'Number of %s:' % (P_CENSUS_LEVEL)
    print df.groupby('STUSAB').size()
    if P_GENERATE_ALL_THE_CENSUS_LEVEL is True:
        print 'According to input data (user settings)'
    else:     
        print 'Check Tallies here :'
        print 'https://www.census.gov/geo/maps-data/data/tallies/tractblock.html'
    print '----------------------------------------------'
    


def log__step(task,params,date,status,nb_records,state,comment, **kwargs):
    
    d_log={
        'task':task
        ,'params':params
        ,'date':date
        ,'status':status
        ,'nb_records':nb_records
        ,'state':state
        ,'comment':comment
    }
    
    for (k, v) in kwargs.items():
        d_log[k] = v
        
    df = pd.DataFrame([d_log]) 
            
    return df

def make__log(df_log,task,params,date,status,nb_records,state,comment, **kwargs):
    df_log_tmp = log__step(task,params,date,status,nb_records,state,comment, **kwargs)
    df_log = pd.concat((df_log,df_log_tmp),axis=0)
    return df_log


def create_output_batch(outcolz_list,P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT,output_key_list):
    nc = len(outcolz_list)
    nv = nc / P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT

    d={'i':[],'col':[]}
    for i,col in enumerate(outcolz_list):
        nv = (i+1) / P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT
        d['i'].append(nv)
        d['col'].append(col)

    d_df = pd.DataFrame(d)

    d_out = dict()
    for ei in np.unique(d['i']):
        d_dfi = d_df[d_df['i']==ei]
        d_list = list(d_dfi['col'])
        d_out[ei] = output_key_list + d_list
    dlast = ei
    
    return d_out,dlast
