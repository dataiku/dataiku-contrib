import dataiku
from dataiku.customrecipe import *

print "Hello, my config is %s" % get_recipe_config()
print "I will load %s" % get_input_names()

compute_stddev = get_recipe_config()["computeStdDev"]

inp = dataiku.Dataset(get_input_names()[0]).get_dataframe()

audit = dataiku.pandasutils.audit(inp)

dataiku.Dataset(get_output_names()[0]).write_with_schema(audit)