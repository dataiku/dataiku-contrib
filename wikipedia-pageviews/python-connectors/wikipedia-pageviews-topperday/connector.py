from dataiku.connector import Connector
import datetime
import dkuwikipedia

class TopPerDayConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)

    def get_read_schema(self):
        return {"columns" : [
                    {"name" : "project", "type" : "string"},
                    {"name" : "page", "type" : "string"},
                    {"name" : "date", "type" : "date"},
                    {"name" : "views", "type" : "bigint"},
                    {"name" : "rank", "type" : "int"}
                ]}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        (beg_date, end_date) = dkuwikipedia.get_daterange(self.config)
        projects = dkuwikipedia.get_projects(self.config)

        cur_date = beg_date
        while cur_date < end_date:
            for project in projects:

                print "Query for %s : %s" % (cur_date, project)
                resp = dkuwikipedia.query_top(project, cur_date)
                dic = resp.json()
                for item in dic.get("items", [{"articles": []}])[0]["articles"]:
                    yield {
                        "project" : project,
                        "date" : dkuwikipedia.format_date(cur_date),
                        "page" : item["article"],
                        "views" : item["views"],
                        "rank" : item["rank"]
                    }
            cur_date = cur_date + datetime.timedelta(days=1)
