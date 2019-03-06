# coding: utf-8
from dataiku.sql import Column, Constant, Expression, Interval, SelectQuery, Window, TimeUnit, JoinTypes, ColumnType, toSQL
from dataiku.core.sql import SQLExecutor2, HiveExecutor

class dialectHandler():
    
    def __init__(self, dataset):
        self.dataset = dataset
        self.is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
        self.executor = HiveExecutor(dataset=dataset) if self.is_hdfs else SQLExecutor2(dataset=dataset)
        
    def convertToSQL(self, sql_object):
        if self.is_hdfs:
            return toSQL(sql_object, dialect='Hive')
        else:
            return toSQL(sql_object, dataset=self.dataset)
        
    def get_executor(self):
        return self.executor 

    def execute_in_database(self, query, output_dataset=None):
        if self.is_hdfs:
            self.executor.exec_recipe_fragment(query)
        else:
            self.executor.exec_recipe_fragment(output_dataset=output_dataset, query=query)