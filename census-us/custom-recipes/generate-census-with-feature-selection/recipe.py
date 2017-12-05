# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import numpy as np
import os
import census_resources
import common
from dataiku.customrecipe import *
import feature_selection
import census_metadata
import random

process_date = common.get_date()

#------------------------------------------- SETTINGS
P_COLUMN_STATES = get_recipe_config()['param_column_state']  ### column indicating the state to be created
P_COLUMN_STATES_LOWER = True ### check box : do we need to lower your state code ? 'DC' to 'dc' ?
P_STATES_TYPE_NAME = get_recipe_config()['param_state_format'] ### type of state label : is it DC or DistrictOfColumbia ?

## input dataset content
P_ID_COL = get_recipe_config()['param_column_id'] 
P_CENSUS_LEVEL_COLUMN = get_recipe_config()['param_census_level_column'] #'block_group' ### Column containing a level to be considered. If empty : ALL


# US Census content
P_CENSUS_LEVEL = get_recipe_config()['param_census_level'] #'BLOCK_GROUP' #'TRACT' #...
P_CENSUS_CONTENT = get_recipe_config()['param_census_content'] #'ACS5Y2014' ## census content & vintage
P_CENSUS_TYPE = P_CENSUS_CONTENT[:3] 


# Feature selection settings
P_TARGET = get_recipe_config()['param_column_target'] #'price_by_sqft'  ### target column for feature selection

columns = [P_ID_COL,P_COLUMN_STATES,P_CENSUS_LEVEL_COLUMN,P_TARGET]


P_SUPERVISION_TYPE = get_recipe_config()['param_feature_selection_superv_type'].split(' - ')[0] #'REGRESSION'
P_SUPERVISION_ALGO = get_recipe_config()['param_feature_selection_superv_type'].split(' - ')[1]


# imputation strategy
P_strategy= get_recipe_config()['param_impute_strategy']
P_threshold= float(get_recipe_config()['param_imputation_threshold']) / 100
P_let_below_threshold=False

#P_FEATURE_SELECTION_MODE = get_recipe_config()['param_feature_selection_superv_type'] #'REGRESSION'
P_FEATURE_SELECTION_THRESHOLD = float(get_recipe_config()['param_sig_threshold'])/100 #0.05
P_FEATURE_SELECTION_NB_FIELD_MAX = int(get_recipe_config()['param_nb_fields_max']) #-1 ## -1 = ALL

P_RESCALE = get_recipe_config()['param_rescale']

### Output gen
P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT = int(get_recipe_config()['param_nb_field_per_output']) #200
P_GEN_UNIQUE_FILE = get_recipe_config()['param_output_one_file_all_states'] #True
P_GENERATE_ALL_THE_CENSUS_LEVEL = get_recipe_config()['param_output_only_matching_census_level'] #False

## Re us DL content
P_USE_PREVIOUS_SOURCES = get_recipe_config()['param_re_use_collected_census_sources']   
P_DELETE_US_CENSUS_SOURCES = get_recipe_config()['param_delete_census_sources']


#------------------------------------------- END SETTINGS

path_datadir_tmp = dataiku.get_custom_variables()["dip.home"] + '/tmp/'
FOLDER_NAME = 'tmp_census_us_'+ P_CENSUS_CONTENT 


input_ = get_input_names_for_role('input')[0]

output_folder = dataiku.Folder(get_output_names_for_role('censusdata')[0])
path_ = output_folder.get_path() + '/'

print 'Checking if previous files exist...'
if len(os.listdir(path_))>0:
    for fz in os.listdir(path_):
        cmd = "rm %s" % (path_ + fz)
        print 'removing: %s' % (fz)
        os.system(cmd)



dict_states = common.get_state_structure(P_STATES_TYPE_NAME)

CENSUS_TYPE = str(census_resources.dict_vintage_[P_CENSUS_CONTENT[:3]])

params = {
    'level_column':P_CENSUS_LEVEL_COLUMN
    ,'level':P_CENSUS_LEVEL
    ,'content':P_CENSUS_CONTENT
    ,'tmp_storage':path_datadir_tmp
    ,'mode':'feature selection'
    ,'algo': get_recipe_config()['param_feature_selection_superv_type']
    ,'threshold':P_FEATURE_SELECTION_THRESHOLD
    ,'nb_top_features':P_FEATURE_SELECTION_NB_FIELD_MAX
    ,'nb_features_per_output':P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT 
    ,'unique_file':P_GEN_UNIQUE_FILE 
    ,'only_matching_level':P_GENERATE_ALL_THE_CENSUS_LEVEL 
    ,'target':P_TARGET
    ,'imputing':P_strategy
    ,'imputing_threshold':P_threshold
    ,'rescale':P_RESCALE
    
    }


df_log_ = common.log__step('0',params,process_date,'',0,'','init')


#----------------------------------------- INPUT DATASET

print '0/6 Processing input dataset...'

df  = dataiku.Dataset(input_).get_dataframe(columns=columns)
if P_COLUMN_STATES_LOWER is True:
    df[P_COLUMN_STATES] = df[P_COLUMN_STATES].map(lambda x: x.lower())
    
    
print 'Creating States list...'
state_list_  = list(np.unique(df[P_COLUMN_STATES]))


state_conversion = common.state_to_2letters_format(P_STATES_TYPE_NAME, state_list_)

state_list = state_conversion[0]
state_list_rejected = state_conversion[1]
dict_states = state_conversion[2]

s_found = len(state_list)
s_rejected = len(state_list_rejected)

print '----------------------------------------'
print 'First diagnostic on input dataset'
print '----------------------------------------'



##### define the folder containing the segments.
template_fields_def = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']
seq_folder_name = template_fields_def['folder_name']
## For taking into account that some vintage like ACS52013 does not have a structure with the template and a folder
## If no template, recreate the same structure as the alternative one.
if seq_folder_name=='':
    seq_folder_name = template_fields_def['geo_header_template_folder_name']

    
if s_found >0:
    print 'States expected to be processed if enough records for feature selection:'
    print state_list
    df_log_ = common.make__log(df_log_,'1',state_list,process_date,'OK',len(state_list),'','States in Feature selection')
    print 'States rejected (state does not exist):'
    if s_rejected < 60:
        print state_list_rejected
    else:
        print '...too many elements rejected for displaying it in the log...'
    
    

    ### create the dataset for feature selection
    keep_columns_list = [P_CENSUS_LEVEL_COLUMN,P_TARGET,P_ID_COL,P_COLUMN_STATES]
    df2 = df[keep_columns_list]

    df2_unique = df2.groupby(P_CENSUS_LEVEL_COLUMN).size().reset_index()
    df2_unique = df2_unique[[P_CENSUS_LEVEL_COLUMN]]
    df2_unique[P_CENSUS_LEVEL_COLUMN] = df2_unique[P_CENSUS_LEVEL_COLUMN].astype('int64')

    df2n = df2.groupby(P_COLUMN_STATES).size().reset_index()
    df2n.rename(columns={0: 'nb'}, inplace=True)

    
    #----------------------------------------- BASE FOLDER

    print '1/6 Creating base folders...' 

    common.create_folder(path_datadir_tmp,FOLDER_NAME,False)  

    common.create_folder(path_datadir_tmp + '/' + FOLDER_NAME +'/',P_CENSUS_LEVEL,False)



    #----------------------------------------- SOURCE HARVESTER

    if P_USE_PREVIOUS_SOURCES is False:
        print '2/6 Collecting US Census Data...' 

    else:
        print '2/6 Re using US Census Data if available...'

    sources_collector = common.us_census_source_collector(P_USE_PREVIOUS_SOURCES,P_CENSUS_TYPE,P_CENSUS_CONTENT,P_CENSUS_LEVEL,path_datadir_tmp,FOLDER_NAME,state_list,dict_states)

    sumlevel_val= sources_collector[0]
    fdef_dir= sources_collector[1]
    geo_header_file= sources_collector[2]
    dict_pattern_files= sources_collector[3]


    geo_header_file_dir = fdef_dir + '/' + geo_header_file 
    
    
    ##debug
    print '-------------- GEO HEADER -----------------'
    print geo_header_file_dir
    print '=='
    print fdef_dir
    print geo_header_file
    
    geo_header = pd.read_excel(geo_header_file_dir, sheetname=0, header=0)

    
    #----------------------------------------- FEATURE SELECTION

    print '3/6 Feature selection...'

    census_level_code_len = census_resources.dict_level_corresp['v1'][P_CENSUS_LEVEL]['code_len']

    #### take the state if there is more than X rows in the input dataset
    df2n_ok = df2n[df2n['nb']>30]
    df2n_ok = df2n_ok[P_COLUMN_STATES].reset_index()
    n_state_list = list(df2n_ok[P_COLUMN_STATES])


    dict_features = {#'state':[],
                     'predictor':[]
                     ,'pvalue':[]
                     #,'feature_catalog': [] ## future usage
                     ,'created_at': []
                     , 'segment_number':[]
                     , 'nrows':[]

                    }
        
    for state in state_list:

        print 'Processing this state: %s' % (state)

        state_dir = path_datadir_tmp + FOLDER_NAME+'/'+ state 

        if P_CENSUS_LEVEL in ('TRACT','BLOCK_GROUP'):
            ziptocollect = dict_pattern_files['v1']['TB']
            state_dir_level = state_dir +'/'+ 'TRACT_BG_SEG'

        else:
            ziptocollect = dict_pattern_files['v1']['OT']
            state_dir_level = state_dir +'/'+ 'NO_TRACT_BG_SEG'


        print 'Starting feature selection...'
        if state in n_state_list:

            ustate = state.upper()

            state_name = dict_states[state]['attributes']['state_fullname_w1']
            state_number = dict_states[state]['attributes']['state_2digits']

            print 'Feature selection for state = %s' % state_name

            vint = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]
            master_segment_file = state_dir_level + '/'  + vint['master_segment_file_pattern']+ vint['vintage_pattern']+state+'.csv'

            geo_source_df = pd.read_csv(master_segment_file, sep =',', header = None, names = geo_header.columns)
            
            ## debug
            NNN = geo_source_df.shape
            
            print '---------------------------------'
            print master_segment_file
            print geo_header_file_dir
            
            print '---------------------------------'
            print geo_header.columns
            
            print '---------------------------------'
            print sumlevel_val
            
            print '---------------------------------'
            print NNN
            
            print '---------------------------------'
            print geo_source_df.head(2)
            
            geo_level_df = geo_source_df[geo_source_df['SUMLEVEL'].isin(sumlevel_val)].copy()
            geo_level_df['GEOID_DKU'] = geo_level_df['GEOID'].map(lambda x: x.split('US')[1])


            geo_level_df[P_CENSUS_LEVEL] = geo_level_df['GEOID_DKU'].map(lambda x: x[:census_level_code_len])

            keep_cols = ['FILEID','SUMLEVEL','GEOID_DKU','STUSAB','LOGRECNO']
            geo_level_df = geo_level_df[keep_cols]
            geo_level_df['LOW_STUSAB'] = state #geo_level_df['STUSAB'].map(lambda x: x.lower()) ## basically the state lower


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


            ns = len(segment_list)
            
            for i,segment_number in enumerate(segment_list):

                print 'Processing segment %s/%s' % (i+1,ns)


                #seq_folder_name = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]['fields_definition']['folder_name']
                HEADER_PATH_FILE = fdef_dir + '/'+ seq_folder_name +'/Seq' + str(int(segment_number)) + '.xls'
                header_df = pd.read_excel(HEADER_PATH_FILE,sheetname=0) ### 0 = 'E'

                ### Adjust the header to fit what we need.
                kh_list = ['FILEID', 'FILETYPE', 'STUSAB', 'CHARITER', 'SEQUENCE', 'LOGRECNO']
                f_list = [x for x in header_df.columns if x not in kh_list]
                E_list = [x + 'E' for x in f_list]
                newcolz_list = kh_list + E_list

                SEGMENT_PATH_FILE = state_dir_level  + '/' +  vint['segments_estimations_files_pattern']+ vint['vintage_pattern'] + state + segment_number + '000.txt'
                segment_df = pd.read_csv(SEGMENT_PATH_FILE, sep = ',', names = newcolz_list,low_memory=False)

                #### later: impute missing values with median or avg.
                data_cols = []
                for column in segment_df.columns:
                    if column not in kh_list:
                        if segment_df[column].dtype != 'object':
                            data_cols.append(column)


                geo_level_df_fs = pd.merge(left= geo_level_df, right= segment_df, how='inner', left_on = ['LOW_STUSAB','LOGRECNO'], right_on = ['STUSAB','LOGRECNO'])


                keep_fs_tmp = []
                keep_fs_tmp = ['GEOID_DKU'] + data_cols        

                features_df = geo_level_df_fs[keep_fs_tmp]

                #### Find the columns with No values = 100% NaN
                features_df_desc = features_df.describe().T
                features_df_desc = features_df_desc[features_df_desc['count']==0].reset_index()
                keep_fs_tmp2 = [x for x in features_df.columns if x not in features_df_desc['index'].tolist()]
                features_df2 = features_df[keep_fs_tmp2]

                
                remaining_data_cols = [v for v in keep_fs_tmp2 if v not in kh_list + ['GEOID_DKU']]
                
                #### Imputation strategy
                if P_strategy !='No':
                    
                    if len(remaining_data_cols)>0:
                        features_df2 = common.impute_strategy(P_strategy,P_threshold,features_df2,remaining_data_cols,P_let_below_threshold)

                    else:
                        features_df2 = features_df2.dropna() #common.df_dropna(features_df2)
                
                if P_RESCALE is True:
                    features_df2 = common.rescaling(features_df2,remaining_data_cols)
                
                features_df2['GEOID_DKU'] = features_df2['GEOID_DKU'].astype('int64')

                df_for_fs = pd.merge(df2,features_df2, how='inner', left_on= P_CENSUS_LEVEL_COLUMN ,right_on='GEOID_DKU' )

                nrows = df_for_fs.shape[0]
                ncols = df_for_fs.shape[1]

                if nrows > 100 & ncols > 1:

                    data_cols2 = [x for x in df_for_fs.columns if x not in ['GEOID_DKU',P_TARGET,P_COLUMN_STATES,P_ID_COL,P_CENSUS_LEVEL_COLUMN]]

                    if len(data_cols2)>0:
                        
                        try:
                            pval = feature_selection.univariate_feature_selection(P_SUPERVISION_ALGO,df_for_fs[data_cols2], df_for_fs[P_TARGET])

                            for c in zip(data_cols2,pval):
                                dict_features['predictor'].append(c[0])
                                dict_features['pvalue'].append(c[1])
                                #dict_features['state'].append(state)
                                dict_features['segment_number'].append(segment_number)
                                dict_features['created_at'].append(process_date)
                                #dict_features['feature_catalog'].append('') ## future usage
                                dict_features['nrows'].append(nrows)
                        except:
                            print 'Warning - Feature selection issue for this segment, you should consider another imputation strategy'
                            params={'segment_number':segment_number, 'columns':data_cols2}
                            df_log_ = common.make__log(df_log_,'fs issue',params,process_date,'Warning',-1,state,'segments')
                            
        else:
            print 'For state = %s, too few values for performing the feature selection' % (state)
            params={'reason':'state <30 rows'}
            df_log_ = common.make__log(df_log_,'too few values for feature selection',params,process_date,'KO',-1,state,'requirements')

    features_scores_df = pd.DataFrame(dict_features)
    features_scores_df['feature_kept'] = features_scores_df['pvalue'].map(lambda x: 1 if (x <= P_FEATURE_SELECTION_THRESHOLD) else 0 )
    features_kept_df = features_scores_df[features_scores_df['feature_kept']==1]
    features_kept_df = features_kept_df.sort_values('pvalue', ascending=[1]).reset_index()
    features_kept_df = features_kept_df.reset_index() ## get the ranking
    del features_kept_df['index']
    features_kept_df.rename(columns={'level_0': 'feature_ranking'}, inplace=True)

    if P_FEATURE_SELECTION_NB_FIELD_MAX >= 0:
        features_kept_df = features_kept_df.sort_values('pvalue', ascending=[1])[:P_FEATURE_SELECTION_NB_FIELD_MAX]

    
    ### we take all the significative features whatever the performance regarding the state.
    features_list = pd.unique(features_kept_df['predictor'])
    n_features = len(features_list)

    print 'Nb features selected: %s' % (n_features)

    dico_features_kept = {k: list(v) for k,v in features_kept_df.groupby("segment_number")["predictor"]}
    nb_segments = len(dico_features_kept)

    print 'Nb corresponding segments: %s' % (nb_segments)


    #----------------------------------------- GENERATE FILES

    print '4/6 Generating census...' 

    if P_GEN_UNIQUE_FILE is True:
        final_output_df = pd.DataFrame() 

    
    for state in state_list:
        

        print 'Processing this state: %s' % (state)

        state_dir = path_datadir_tmp + FOLDER_NAME+'/'+ state 

        if P_CENSUS_LEVEL in ('TRACT','BLOCK_GROUP'):
            ziptocollect = dict_pattern_files['v1']['TB']
            state_dir_level = state_dir +'/'+ 'TRACT_BG_SEG'

        else:
            ziptocollect = dict_pattern_files['v1']['OT']
            state_dir_level = state_dir +'/'+ 'NO_TRACT_BG_SEG'


        ustate = state.upper()

        state_name = dict_states[state]['attributes']['state_fullname_w1']
        state_number = dict_states[state]['attributes']['state_2digits']

        vint = census_resources.dict_vintage_[P_CENSUS_TYPE][P_CENSUS_CONTENT]
        master_segment_file = state_dir_level + '/'  + vint['master_segment_file_pattern']+ vint['vintage_pattern']+state+'.csv'

        geo_source_df = pd.read_csv(master_segment_file, sep =',', header = None, names = geo_header.columns)
        geo_level_df = geo_source_df[geo_source_df['SUMLEVEL'].isin(sumlevel_val)].copy()
        geo_level_df['GEOID_DKU'] = geo_level_df['GEOID'].map(lambda x: x.split('US')[1])

        geo_level_df[P_CENSUS_LEVEL] = geo_level_df['GEOID_DKU'].map(lambda x: x[:census_level_code_len])

        keep_cols = ['FILEID','SUMLEVEL','GEOID_DKU','STUSAB','LOGRECNO']
        geo_level_df = geo_level_df[keep_cols]
        geo_level_df['STUSAB'] = geo_level_df['STUSAB'].map(lambda x: x.lower()) ## basically the state lower

        del geo_level_df['FILEID']
        del geo_level_df['SUMLEVEL']

        for i,segment_number in enumerate(dico_features_kept.keys()):
            selected_feature_list = dico_features_kept[segment_number]

            print 'Processing segment: %s/%s' % (i+1,nb_segments)


            HEADER_PATH_FILE = fdef_dir + '/'+ seq_folder_name +'/Seq' + str(int(segment_number)) + '.xls'
            header_df = pd.read_excel(HEADER_PATH_FILE,sheetname=0) ### 0 = 'E'

            ### Adjust the header to fit what we need.
            kh_list = ['FILEID', 'FILETYPE', 'STUSAB', 'CHARITER', 'SEQUENCE', 'LOGRECNO']
            f_list = [x for x in header_df.columns if x not in kh_list]
            E_list = [x + 'E' for x in f_list]
            newcolz_list = kh_list + E_list

            SEGMENT_PATH_FILE = state_dir_level  + '/' +  vint['segments_estimations_files_pattern']+ vint['vintage_pattern'] + state + segment_number + '000.txt'
            segment_df = pd.read_csv(SEGMENT_PATH_FILE, sep = ',', names = newcolz_list,low_memory=False)

            segment_df = segment_df[kh_list + selected_feature_list]

            del segment_df['FILEID']
            del segment_df['FILETYPE']
            del segment_df['CHARITER']
            del segment_df['SEQUENCE']


            geo_level_df = pd.merge(left= geo_level_df, right= segment_df, how='inner', left_on = ['STUSAB','LOGRECNO'], right_on = ['STUSAB','LOGRECNO'])


            
            
        if P_GENERATE_ALL_THE_CENSUS_LEVEL is True:
            geo_level_df['GEOID_DKU'] = geo_level_df['GEOID_DKU'].astype('int64')
            geo_level_df = pd.merge(left= geo_level_df, right= df2_unique, how='inner', left_on = 'GEOID_DKU', right_on = P_CENSUS_LEVEL_COLUMN )
            del geo_level_df[P_CENSUS_LEVEL_COLUMN]

        
        #del geo_level_df['STUSAB']
        del geo_level_df['LOGRECNO']

        output_key_list = ['GEOID_DKU','STUSAB']

        if P_GEN_UNIQUE_FILE is True:
            final_output_df = pd.concat((final_output_df,geo_level_df),axis=0)
        
        else:
        
            common.volumes_tallies_printer(geo_level_df,P_GENERATE_ALL_THE_CENSUS_LEVEL,P_CENSUS_LEVEL)
        

            outcolz_list = [x for x in geo_level_df.columns if x not in output_key_list]
            
            output_batch= common.create_output_batch(outcolz_list,P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT,output_key_list)
            d_out = output_batch[0]
            dlast = output_batch[1]

            print '5/6 Output files ...' 

            for b in d_out.keys():
                print 'Exporting part #%s on %s' % (b,dlast)
                fields_out = d_out[b]
                final_output_df_batch = geo_level_df[fields_out]

                filefullpath = path_ + P_CENSUS_CONTENT + '_' + state + '_' + P_CENSUS_LEVEL + '_' + 'fs_' + P_TARGET + '_part' + str(b) + '.csv'
                final_output_df_batch.to_csv(filefullpath,sep=',',index=None) #, header=True)
                    
            
    if P_GEN_UNIQUE_FILE is True:
            
        common.volumes_tallies_printer(final_output_df,P_GENERATE_ALL_THE_CENSUS_LEVEL,P_CENSUS_LEVEL)
                
        outcolz_list = [x for x in final_output_df.columns if x not in output_key_list]
                
        output_batch= common.create_output_batch(outcolz_list,P_FEATURE_SELECTION_NB_FIELD_PER_OUTPUT,output_key_list)
        d_out = output_batch[0]
        dlast = output_batch[1]
            
        for b in d_out.keys():
            print 'Exporting part #%s / %s' % (b,dlast)
            fields_out = d_out[b]
            final_output_df_batch = final_output_df[fields_out]

            filefullpath = path_ + P_CENSUS_CONTENT + '_' + P_CENSUS_LEVEL + '_' + 'fs_' + P_TARGET + '_part' + str(b) + '.csv'
            final_output_df_batch.to_csv(filefullpath,sep=',',index=None) #, header=True)

                       
    print 'All files generated'

    if P_DELETE_US_CENSUS_SOURCES is True:

        print '6/6 Removing US Census temp data from: %s' % (path_datadir_tmp + FOLDER_NAME)
        cmd = "rm -rf %s" % (path_datadir_tmp + FOLDER_NAME)
        os.system(cmd)

    else:
        print '6/6 Keeping US Census data sources in: %s' % (path_datadir_tmp + FOLDER_NAME)
        for f in os.listdir(path_datadir_tmp + FOLDER_NAME):
            if not f.endswith('.zip'):
                cmd = "rm -rf %s" % (path_datadir_tmp + FOLDER_NAME + '/' + f)
                os.system(cmd)


                
                
    #----------------------------------------- METADATA
    
    
    output_feature_selection_report = get_output_names_for_role('feature_selection_report')[0]
    if len(output_feature_selection_report)>0:
        feature_selection_dataset = dataiku.Dataset(output_feature_selection_report)
    
        
        url_metadata = vint['variables_definitions']
        metadata_results = census_metadata.get_metadata_sources(url_metadata)

        metadata_status = metadata_results[0]
        df_metadata_source = metadata_results[1]

        if metadata_status =='ok':
            
            features_kept_df = pd.merge(features_kept_df,df_metadata_source, left_on='predictor', right_on='name', how='inner')
            
            del df_metadata_source
            #del df_metadata
            
        else:
            print metadata_status
            df_log_ = common.make__log(df_log_,'',metadata_status,process_date,'KO',0,'','failed metadata')
        
        features_kept_df['census']=P_CENSUS_CONTENT
        
        print 'Exporting feature selection report...' 
        feature_selection_dataset.write_with_schema(features_kept_df)
    
    #--------------------------------------------------
    
                
    print 'Flushing pandas dataframes...'
    del df
    del df2
    del df2_unique
    del df2n
    del df2n_ok
    del segment_df
    del geo_level_df
    del features_scores_df
    del features_kept_df
    #del d_df
    try:
        del final_output_df_batch
    except:
        pass
    if P_GEN_UNIQUE_FILE is True:
        del final_output_df

    
    df_log_ = common.make__log(df_log_,'999','',process_date,'OK',0,'','end')
    print '----------------------------------------'
    print 'US Census with feature selection created'
    print '----------------------------------------'
    
    
    
else:
    print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    print 'US Census CANNOT be built, no states available in your dataset...'
    print 'Check the following settings :'
    print '-> are the states in the right format regarding the plugin set by the user ?'
    print '-> is the column really containing states ?'
    print '----------------------------------------'
    df_log_ = common.make__log(df_log_,'999','',process_date,'KO',0,'','end with no states')
    

output_log = get_output_names_for_role('log')
if len(output_log) >0:
    log_dataset = dataiku.Dataset(output_log[0])
    log_dataset.write_with_schema(df_log_)

