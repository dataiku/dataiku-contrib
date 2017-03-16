# -*- coding: utf-8 -*-
from dataiku.connector import Connector
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import numpy as np
import os
import census_resources
import common
import census_metadata


class USCensusConnector(Connector):

    def __init__(self, config, plugin_config):

        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # perform some more initialization
        self.P_state_list_str = str(self.config.get("param_state_list")) #, "defaultValue")
        self.P_STATES_TYPE_NAME = self.config.get("param_state_format")
        self.P_CENSUS_CONTENT = self.config.get("param_census_content")
        self.P_CENSUS_LEVEL = self.config.get("param_census_level")
        self.P_census_fields = str(self.config.get("param_fields"))
        self.P_USE_PREVIOUS_SOURCES = self.config.get("param_re_use_collected_census_sources")
        self.P_DELETE_US_CENSUS_SOURCES = self.config.get("param_delete_census_sources")

    def get_read_schema(self):
        
        fields_list = self.P_census_fields.split(',')
        
        P_CENSUS_TYPE = self.P_CENSUS_CONTENT[:3]
        vint = census_resources.dict_vintage_[P_CENSUS_TYPE][self.P_CENSUS_CONTENT]
        url_metadata = vint['variables_definitions']
        
        metadata_results = census_metadata.get_metadata_sources(url_metadata)

        metadata_status = metadata_results[0]
        df_metadata_source = metadata_results[1]
        
        mlist = list(df_metadata_source['name'])
        
        if metadata_status =='ok':
            ok_fields_list = [c for c in fields_list if c in mlist]
            all_fields_list = ['GEOID_DKU','STUSAB'] + ok_fields_list
            
        else:
            print metadata_status
            all_fields_list = ['GEOID_DKU','STUSAB'] + fields_list
        
        if self.P_STATES_TYPE_NAME is not 'state_2letters': 
            all_fields_list = all_fields_list + [self.P_STATES_TYPE_NAME]
        
        
        l=[]
        for field in all_fields_list:
            d={}
            d['name']=field
            d['type']='string'
            l.append(d)

        d_={"columns": l} 

        return d_

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        
        path_datadir_tmp = os.getenv("DIP_HOME")  + '/tmp/'
        FOLDER_NAME = 'tmp_census_us_'+ self.P_CENSUS_CONTENT 
        
        P_CENSUS_TYPE = self.P_CENSUS_CONTENT[:3]
        CENSUS_TYPE = str(census_resources.dict_vintage_[self.P_CENSUS_CONTENT[:3]])
        
        fields_list = self.P_census_fields.split(',')
        
        #----------------------------------------- BASE FOLDER

        print '1/6 Creating base folders...' 

        common.create_folder(path_datadir_tmp,FOLDER_NAME,False)  

        common.create_folder(path_datadir_tmp + '/' + FOLDER_NAME +'/',self.P_CENSUS_LEVEL,False)
        
        
        #----------------------------------------- SOURCE HARVESTER

        state_list_ = self.P_state_list_str.split(',')
                
        state_conversion = common.state_to_2letters_format(self.P_STATES_TYPE_NAME, state_list_)

        state_list = state_conversion[0]
        state_list_rejected = state_conversion[1]
        dict_states = state_conversion[2]
        
        s_found = len(state_list)
        s_rejected = len(state_list_rejected)

        print '----------------------------------------'
        print 'First diagnostic on input dataset'
        print '----------------------------------------'
        if s_found >0:
            print 'States expected to be processed if enough records for feature selection:'
            print state_list
            print 'States rejected:'
            if s_rejected < 60:
                print state_list_rejected
            else:
                print '...too many elements rejected for displaying it in the log...'
        
            if self.P_USE_PREVIOUS_SOURCES is False:
                print '2/6 Collecting US Census Data...' 

            else:
                print '2/6 Re using US Census Data if available...'

            sources_collector = common.us_census_source_collector(self.P_USE_PREVIOUS_SOURCES,P_CENSUS_TYPE,self.P_CENSUS_CONTENT,self.P_CENSUS_LEVEL,path_datadir_tmp,FOLDER_NAME,state_list,dict_states)

            sumlevel_val= sources_collector[0]
            fdef_dir= sources_collector[1]
            geo_header_file= sources_collector[2]
            dict_pattern_files= sources_collector[3]
            #status= sources_collector[4]


            geo_header_file_dir = fdef_dir + '/' + geo_header_file       
            geo_header = pd.read_excel(geo_header_file_dir, sheetname=0, header=0)


            census_level_code_len = census_resources.dict_level_corresp['v1'][self.P_CENSUS_LEVEL]['code_len']

            print '4/6 Generating census...' 


            final_output_df = pd.DataFrame() 

            for state in state_list:

                print 'Processing this state: %s' % (state)

                state_dir = path_datadir_tmp + FOLDER_NAME+'/'+ state 

                if self.P_CENSUS_LEVEL in ('TRACT','BLOCK_GROUP'):
                    ziptocollect = dict_pattern_files['v1']['TB']
                    state_dir_level = state_dir +'/'+ 'TRACT_BG_SEG'

                else:
                    ziptocollect = dict_pattern_files['v1']['OT']
                    state_dir_level = state_dir +'/'+ 'NO_TRACT_BG_SEG'


                ustate = state.upper()

                state_name = dict_states[state]['attributes']['state_fullname_w1']
                state_number = dict_states[state]['attributes']['state_2digits']

                vint = census_resources.dict_vintage_[P_CENSUS_TYPE][self.P_CENSUS_CONTENT]
                master_segment_file = state_dir_level + '/'  + vint['master_segment_file_pattern']+ vint['vintage_pattern']+state+'.csv'

                geo_source_df = pd.read_csv(master_segment_file, sep =',', header = None, names = geo_header.columns)
                geo_level_df = geo_source_df[geo_source_df['SUMLEVEL'].isin(sumlevel_val)].copy()
                geo_level_df['GEOID_DKU'] = geo_level_df['GEOID'].map(lambda x: x.split('US')[1])

                geo_level_df[self.P_CENSUS_LEVEL] = geo_level_df['GEOID_DKU'].map(lambda x: x[:census_level_code_len])

                keep_cols = ['FILEID','SUMLEVEL','GEOID_DKU','STUSAB','LOGRECNO']
                geo_level_df = geo_level_df[keep_cols]
                geo_level_df['STUSAB'] = geo_level_df['STUSAB'].map(lambda x: x.lower()) ## basically the state lower

                del geo_level_df['FILEID']
                del geo_level_df['SUMLEVEL']

                ### added
                n=0
                for fr in os.listdir(state_dir_level):
                    if fr.startswith(vint['segments_estimations_files_pattern']):
                        n+=1

                segment_list=[]
                for i in range(1,n+1):
                    if i < 10:
                        segment_list.append('000' + str(i))
                    if i in range(10,100):
                        segment_list.append('00' + str(i))
                    if i >= 100:
                        segment_list.append('0' + str(i))

                nb_segments = len(segment_list)

                i=1
                for segment_number in segment_list:

                    i=i+1
                    print 'Processing segment: %s/%s' % (i,nb_segments)

                    
                    template_fields_def = census_resources.dict_vintage_[P_CENSUS_TYPE][self.P_CENSUS_CONTENT]['fields_definition']
                    
                    seq_folder_name = template_fields_def['folder_name']
                    
                    ## For taking into account that some vintage like ACS52013 does not have a structure with the template and a folder
                    ## If no template, recreate the same structure as the alternative one.
                    if seq_folder_name=='':
                        seq_folder_name = template_fields_def['geo_header_template_folder_name']
                        
                    
                    HEADER_PATH_FILE = fdef_dir + '/'+ seq_folder_name +'/Seq' + str(int(segment_number)) + '.xls'
                    header_df = pd.read_excel(HEADER_PATH_FILE,sheetname=0) ### 0 = 'E'

                    ### Adjust the header to fit what we need.
                    kh_list = ['FILEID', 'FILETYPE', 'STUSAB', 'CHARITER', 'SEQUENCE', 'LOGRECNO']
                    f_list = [x for x in header_df.columns if x not in kh_list]
                    E_list = [x + 'E' for x in f_list]
                    newcolz_list = kh_list + E_list
                    
                    t_ = [c for c in newcolz_list if c in fields_list]
                    
                    if len(t_) >0:
                    
                        SEGMENT_PATH_FILE = state_dir_level  + '/' +  vint['segments_estimations_files_pattern']+ vint['vintage_pattern'] + state + segment_number + '000.txt'
                        segment_df = pd.read_csv(SEGMENT_PATH_FILE, sep = ',', names = newcolz_list,low_memory=False)

                        out_list = kh_list + t_
                        out_list.remove('FILEID')
                        out_list.remove('FILETYPE')
                        out_list.remove('CHARITER')
                        out_list.remove('SEQUENCE')
                        
                        segment_df = segment_df[out_list]

                        geo_level_df = pd.merge(left= geo_level_df, right= segment_df, how='inner', left_on = ['STUSAB','LOGRECNO'], right_on = ['STUSAB','LOGRECNO'])


                print '-------------- volumes check------------------'
                print geo_level_df.groupby('STUSAB').size() 
                print 'Check Tallies here :'
                print 'https://www.census.gov/geo/maps-data/data/tallies/tractblock.html'
                print '----------------------------------------------'

                #del geo_level_df['STUSAB']
                del geo_level_df['LOGRECNO']

                if self.P_STATES_TYPE_NAME is not 'state_2letters':
                    geo_level_df[self.P_STATES_TYPE_NAME] = dict_states[state]['attributes'][self.P_STATES_TYPE_NAME]
                    

                print '5/6 Building final output...' 
                final_output_df = pd.concat((final_output_df,geo_level_df),axis=0)



            if self.P_DELETE_US_CENSUS_SOURCES is True:

                print '6/6 Removing US Census temp data from: %s' % (path_datadir_tmp + FOLDER_NAME)
                cmd = "rm -rf %s" % (path_datadir_tmp + FOLDER_NAME)
                os.system(cmd)

            else:
                print '6/6 Keeping US Census data sources in: %s' % (path_datadir_tmp + FOLDER_NAME)
                for f in os.listdir(path_datadir_tmp + FOLDER_NAME):
                    if not f.endswith('.zip'):
                        cmd = "rm -rf %s" % (path_datadir_tmp + FOLDER_NAME + '/' + f)
                        os.system(cmd)


            for i, line in final_output_df.iterrows():
                yield line.to_dict()

        else:
            print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
            print 'US Census CANNOT be built, no states available in your dataset...'
            print 'Check the following settings :'
            print '-> are the states in the right format regarding the plugin set by the user ?'
            print '-> is the column really containing states ?'
            print '----------------------------------------'
            



    
