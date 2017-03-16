# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *


dict_level_corresp ={'v1':
                     {'alaska native regional corporation':'alaska native regional corporation'
                        ,'american indian area (off-reservation trust land only)/hawaiian home land':'american indian area (off-reservation trust land only)/hawaiian home land'
                        ,'american indian area/alaska native area (reservation or statistical entity only)':'american indian area/alaska native area (reservation or statistical entity only)'
                        ,'american indian area/alaska native area/hawaiian home land':'american indian area/alaska native area/hawaiian home land'
                        ,'BLOCK_GROUP':{'census_name':'block group','code_len':12}
                        ,'combined new england city and town area':'combined new england city and town area'
                        ,'combined statistical area':'combined statistical area'
                        ,'congressional district':'congressional district'
                        ,'consolidated city':'consolidated city'
                        ,'COUNTY':{'census_name':'county','code_len':5}
                        ,'county subdivision':'county subdivision'
                        ,'DIVISION':'division'
                        ,'metropolitan division':'metropolitan division'
                        ,'metropolitan statistical area/micropolitan statistical area':'metropolitan statistical area/micropolitan statistical area'
                        ,'micropolitan statistical area':'micropolitan statistical area'
                        ,'necta division':'necta division'
                        ,'new england city and town area':'new england city and town area'
                        ,'place':'place'
                        ,'place remainder':'place remainder'
                        ,'place/remainder':'place/remainder'
                        ,'principal city':'principal city'
                        ,'principal city (or part)':'principal city (or part)'
                        ,'public use microdata area':'public use microdata area'
                        ,'region':'region'
                        ,'school district (elementary)':'school district (elementary)'
                        ,'school district (secondary)':'school district (secondary)'
                        ,'school district (unified)':'school district (unified)'
                        ,'STATE':{'census_name':'state','code_len':2}
                        ,'state legislative district (lower chamber)':'state legislative district (lower chamber)'
                        ,'state legislative district (upper chamber)':'state legislative district (upper chamber)'
                        ,'subminor civil division':'subminor civil division'
                        ,'TRACT':{'census_name':'tract','code_len':10}
                        ,'tribal block group':'tribal block group'
                        ,'tribal block group (or part) within tribal census tract within aia (reservation only)':'tribal block group (or part) within tribal census tract within aia (reservation only)'
                        ,'tribal block group (or part) within tribal census tract within aia (trust land only)':'tribal block group (or part) within tribal census tract within aia (trust land only)'
                        ,'tribal census tract':'tribal census tract'
                        ,'tribal census tract (or part) within aia (reservation only)':'tribal census tract (or part) within aia (reservation only)'
                        ,'tribal census tract (or part) within aia (trust land only)':'tribal census tract (or part) within aia (trust land only)'
                        ,'tribal subdivision/remainder':'tribal subdivision/remainder'
                        ,'urban area':'urban area'
                        ,'US':'us'
                        ,'ZCTA':{'census_name':'zip code tabulation area'}
                     }
                    }
                    ### in this dict {'PluginName':'Real level name'}
                    ### Need a check at each API version 


dict_pattern_files_ ={'ACS':{'v1':
                      {
                         'TB':'Tracts_Block_Groups_Only'
                       , 'OT':'All_Geographies_Not_Tracts_Block_Groups'
                      }
                            } 
                     }



dict_vintage_ = {'ACS':{
     'ACS5Y2015':
     {
        'fields_definition':{'url':'http://www2.census.gov/programs-surveys/acs/summary_file/2015/data/2015_5yr_Summary_FileTemplates.zip'
                             ,'folder_name':'2015_5yr_Templates'
                             ,'geo_header_template_url':'http://www2.census.gov/programs-surveys/acs/summary_file/2015/data/2015_5yr_Summary_FileTemplates.zip'
                             ,'geo_header_template_folder_name':'2015_5yr_Templates'
                            }
         ,'ZCTA_TO_ZIPCODE':{'url':'http://udsmapper.org/docs/zip_to_zcta_2016.xlsx','columns_name':['zipcode','po_name','state','zip_type','key']}
         ,'data_by_state':'http://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_by_state/'
         ,'variables_definitions':'http://api.census.gov/data/2015/acs5/variables.html'
         , 'levels_code': 'http://api.census.gov/data/2015/acs5/geography.json'
         , 'vintage_pattern':'20155'
         , 'master_segment_file_pattern':'g'
         , 'segments_estimations_files_pattern':'e'
         , 'segments_margins_files_pattern':'m'
     }
    ,
    'ACS5Y2014':
     {
        'fields_definition':{'url':'http://www2.census.gov/programs-surveys/acs/summary_file/2014/data/2014_5yr_Summary_FileTemplates.zip'
                             ,'folder_name':'seq'
                             ,'geo_header_template_url':'http://www2.census.gov/programs-surveys/acs/summary_file/2014/data/2014_5yr_Summary_FileTemplates.zip'
                             ,'geo_header_template_folder_name':'seq'
                            }
         ,'ZCTA_TO_ZIPCODE':{'url':'http://udsmapper.org/docs/zip_to_zcta_2016.xlsx','columns_name':['zipcode','po_name','state','zip_type','key']}
         ,'data_by_state':'http://www2.census.gov/programs-surveys/acs/summary_file/2014/data/5_year_by_state/'
         ,'variables_definitions':'http://api.census.gov/data/2014/acs5/variables.html'
         , 'levels_code': 'http://api.census.gov/data/2014/acs5/geography.json'
         , 'vintage_pattern':'20145'
         , 'master_segment_file_pattern':'g'
         , 'segments_estimations_files_pattern':'e'
         , 'segments_margins_files_pattern':'m'
     }
    , 'ACS5Y2013':
     {
        'fields_definition':{'url':'http://www2.census.gov/programs-surveys/acs/summary_file/2013/data/2013_5yr_Summary_FileTemplates.zip'
                             ,'folder_name':''  
                             ,'geo_header_template_url':'http://www2.census.gov/programs-surveys/acs/summary_file/2014/data/2014_5yr_Summary_FileTemplates.zip'
                             ,'geo_header_template_folder_name':'seq'
                            } 
         ,'ZCTA_TO_ZIPCODE':{'url':'http://udsmapper.org/docs/zip_to_zcta_2015.xlsx','columns_name':['zipcode','po_name','state','zip_type','key']}
         ,'data_by_state':'http://www2.census.gov/programs-surveys/acs/summary_file/2013/data/5_year_by_state/'
         ,'variables_definitions':'http://api.census.gov/data/2013/acs5/variables.html'
         , 'levels_code': 'http://api.census.gov/data/2013/acs5/geography.json'
         , 'vintage_pattern':'20135'
         , 'master_segment_file_pattern':'g'
         , 'segments_estimations_files_pattern':'e'
         , 'segments_margins_files_pattern':'m'
     }
}}


def get_dict_ref():
    ### do not change or modifiy something in the dict !
    dict_ref = {}

    dict_ref['state_2letters']=['al'
                    ,'ak'
                    ,'az'
                    ,'ar'
                    ,'ca'
                    ,'co'
                    ,'ct'
                    ,'dc'
                    ,'de'
                    ,'fl'
                    ,'ga'
                    ,'hi'
                    ,'id'
                    ,'il'
                    ,'in'
                    ,'ia'
                    ,'ks'
                    ,'ky'
                    ,'la'
                    ,'me'
                    ,'md'
                    ,'ma'
                    ,'mi'
                    ,'mn'
                    ,'ms'
                    ,'mo'
                    ,'mt'
                    ,'ne'
                    ,'nv'
                    ,'nh'
                    ,'nj'
                    ,'nm'
                    ,'ny'
                    ,'nc'
                    ,'nd'
                    ,'oh'
                    ,'ok'
                    ,'or'
                    ,'pa'
                    ,'pr'
                    ,'ri'
                    ,'sc'
                    ,'sd'
                    ,'tn'
                    ,'tx'
                    ,'ut'
                    ,'vt'
                    ,'va'
                    ,'wa'
                    ,'wv'
                    ,'wi'
                    ,'wy']


    dict_ref['state_fullname_w1']=['Alabama'
                    ,'Alaska'
                    ,'Arizona'
                    ,'Arkansas'
                    ,'California'
                    ,'Colorado'
                    ,'Connecticut'
                    ,'DistrictOfColumbia' #### changed vs SF1
                    ,'Delaware'
                    ,'Florida'
                    ,'Georgia'
                    ,'Hawaii'
                    ,'Idaho'
                    ,'Illinois'
                    ,'Indiana'
                    ,'Iowa'
                    ,'Kansas'
                    ,'Kentucky'
                    ,'Louisiana'
                    ,'Maine'
                    ,'Maryland'
                    ,'Massachusetts'
                    ,'Michigan'
                    ,'Minnesota'
                    ,'Mississippi'
                    ,'Missouri'
                    ,'Montana'
                    ,'Nebraska'
                    ,'Nevada'
                    ,'NewHampshire'
                    ,'NewJersey'
                    ,'NewMexico'
                    ,'NewYork'
                    ,'NorthCarolina'
                    ,'NorthDakota'
                    ,'Ohio'
                    ,'Oklahoma'
                    ,'Oregon'
                    ,'Pennsylvania'
                    ,'PuertoRico' #### check ok
                    ,'RhodeIsland' 
                    ,'SouthCarolina'
                    ,'SouthDakota'
                    ,'Tennessee'
                    ,'Texas'
                    ,'Utah'
                    ,'Vermont'
                    ,'Virginia'
                    ,'Washington'
                    ,'WestVirginia'
                    ,'Wisconsin'
                    ,'Wyoming']


    dict_ref['state_2digits']=[
        ##### don't change the ranking , #3 doesn t exist
        '01','02'
        ,'04','05','06','08','09','11' #### changed vs SF1
        ,'10','12'
        ,'13'
        ,'15'
        ,'16'
        ,'17'
        ,'18'
        ,'19'
        ,'20'
        ,'21'
        ,'22'
        ,'23'
        ,'24'
        ,'25'
        ,'26'
        ,'27'
        ,'28'
        ,'29'
        ,'30'
        ,'31'
        ,'32'
        ,'33'
        ,'34'
        ,'35'
        ,'36'
        ,'37'
        ,'38'
        ,'39'
        ,'40'
        ,'41'
        ,'42'
        ,'72' #### check ok
        ,'44' 
        ,'45'
        ,'46'
        ,'47'
        ,'48'
        ,'49'
        ,'50'
        ,'51'
        ,'53'
        ,'54'
        ,'55'
        ,'56'

    ]
    
    return dict_ref


        


        
    