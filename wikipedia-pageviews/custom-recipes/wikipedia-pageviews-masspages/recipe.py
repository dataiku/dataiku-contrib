import dataiku
from dataiku.customrecipe import *
import dkuwikipedia
import logging

logger = logging.getLogger(__name__)


config = get_recipe_config()
(beg_date, end_date) = dkuwikipedia.get_daterange(config)

pages_list_dataset = dataiku.Dataset(get_input_names_for_role('pages_list')[0])

def get_rows():
    for item in pages_list_dataset.iter_rows():
        project = item["project"]
        page = item["page"]

        logger.info("Query for %s : %s" % (project, page))
        resp = dkuwikipedia.query_page(project, page, beg_date, end_date)
        dic = resp.json()
        for item in dic.get("items", []):
            yield {
                "project" : project,
                "date" : dkuwikipedia.parse_and_format_yyyymmddhh(item["timestamp"]),
                "page" : item["article"],
                "views" : item["views"],
            }

out_dataset = dataiku.Dataset(get_output_names_for_role('main')[0])

schema_columns = [
    {"name" : "project", "type" : "string"},
    {"name" : "page", "type" : "string"},
    {"name" : "date", "type" : "date"},
    {"name" : "views", "type" : "bigint"}
]

out_dataset.write_schema(schema_columns)

with out_dataset.get_writer() as wr:
    for row in get_rows():
        wr.write_row_dict(row)
