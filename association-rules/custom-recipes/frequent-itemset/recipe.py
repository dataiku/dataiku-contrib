
# import the classes for accessing DSS objects from the recipe
import dataiku
from dataiku.customrecipe import *


import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from apyori import apriori as ap

## Get Dataset
main_input_name = get_input_names_for_role('transactions')[0]
input_dataset =  dataiku.Dataset(main_input_name)
df_in = input_dataset.get_dataframe()

##Get parameters
support = float(get_recipe_config()['support'])
confidence = float(get_recipe_config()['confidence'])
lift = float(get_recipe_config()['lift'])
max_length = int(get_recipe_config()['max_length'])
min_length = int((get_recipe_config()['min_length'])/2)
grouping_key = get_recipe_config()['grouping_key']

items = get_recipe_config()['items']

##to list
transactions=list(df_in.groupby(grouping_key)[items].apply(list))

list_for_df =[]

##use apyori package
apr =ap(transactions, min_support=support ,max_length=max_length,min_confidence=confidence,min_lift=lift)

## Format result generator into pandas dataframe
for relation in apr:
    for stats in relation.ordered_statistics:
        if len(stats.items_base)>=min_length:
            base = list(stats.items_base)
            add = list(stats.items_add)
        
            ## Unicode problem
            for i in range(len(base)) :
                if isinstance(base[i], str):
                    base[i] = base[i].encode('ascii','ignore')
            for i in range(len(add)) :
                if isinstance(add[i], str):
                    add[i] = add[i].encode('ascii','ignore')

            list_for_df.append((base,add,relation.support,stats.confidence,stats.lift))


##New df
labels = ['left', 'right',  'support','confidence','lift']

if len(list_for_df)==0:
    ##Exception
    df = pd.DataFrame([('No rules found, change paramaters or check the input shape [should be :(id, feature1,feature2,..)] or test association between columns')])
else :
    df = pd.DataFrame.from_records(list_for_df, columns=labels).sort_values(by='confidence',ascending=False)

# Recipe outputs
main_output_name = get_output_names_for_role('rules')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df)