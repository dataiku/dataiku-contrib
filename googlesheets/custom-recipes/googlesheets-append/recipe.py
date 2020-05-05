# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
from googlesheets import get_spreadsheet
from gspread.utils import rowcol_to_a1


# Input
input_name = get_input_names_for_role('input_role')[0]
input_dataset = dataiku.Dataset(input_name)
input_schema = input_dataset.read_schema()


# Output
output_name = get_output_names_for_role('output_role')[0]
output_dataset = dataiku.Dataset(output_name)
output_dataset.write_schema(input_schema)


# Get configuration
config = get_recipe_config()
credentials = config.get("credentials")
doc_id = config.get("doc_id")
tab_id = config.get("tab_id")
insert_format = config.get("insert_format")


# Load worksheet
ws = get_spreadsheet(credentials, doc_id, tab_id)


# Make available a method of later version of gspread (probably 3.4.0)
# from https://github.com/burnash/gspread/pull/556
def append_rows(self, values, value_input_option='RAW'):
    """Adds multiple rows to the worksheet and populates them with values.
    Widens the worksheet if there are more values than columns.
    :param values: List of rows each row is List of values for the new row.
    :param value_input_option: (optional) Determines how input data should
                                be interpreted. See `ValueInputOption`_ in
                                the Sheets API.
    :type value_input_option: str
    .. _ValueInputOption: https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption
    """
    params = {
        'valueInputOption': value_input_option
    }

    body = {
        'values': values
    }

    return self.spreadsheet.values_append(self.title, params, body)

ws.append_rows = append_rows.__get__(ws, ws.__class__)


# Open writer
writer = output_dataset.get_writer()        


# Iteration row by row
batch = []
for row in input_dataset.iter_rows():

    # write to spreadsheet by batch
    batch.append([v for k,v in list(row.items())])

    if len(batch) >= 50:
        ws.append_rows(batch, insert_format)
        batch = []
    
    # write to output dataset
    writer.write_row_dict(row)

if len(batch) > 0:
    ws.append_rows(batch, insert_format)


# Close writer
writer.close()

