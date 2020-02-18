from dataiku.connector import Connector
import datetime
import dkuwikipedia
import logging

logger = logging.getLogger(__name__)

class PagePerDayConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)

    def get_read_schema(self):
        return {"columns" : [
                    {"name" : "project", "type" : "string"},
                    {"name" : "page", "type" : "string"},
                    {"name" : "date", "type" : "date"},
                    {"name" : "views", "type" : "bigint"}
                ]}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        (beg_date, end_date) = dkuwikipedia.get_daterange(self.config)

        for line in self.config.get("pages", "").split("\n"):
            line = line.strip()
            line = line.split(" ")
            project = line[0]
            page = " ".join(line[1:])

            project = project.strip()

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
