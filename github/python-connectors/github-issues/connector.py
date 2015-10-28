import github, json, datetime, itertools, logging, json, pytz
from dataiku.connector import Connector
COLUMNS= [
        ("number", "int"),
        ("title", "string"),
        ("state", "string"),
        ("assignee", "string"),
        ("created_at", "date"),
        ("updated_at", "date"),
        ("closed_at", "date"),
        ("user", "string"),
        ("labels", "string"),
        ("milestone", "string") #, "query_date"
    ]

class GithubIssuesConnector(Connector):
    def __init__(self, config):
        Connector.__init__(self, config)
        gh = github.Github(config["login"], config["password"])
        self.repos = gh.get_repo(config["repos"])


    def get_read_schema(self):
        return { "columns" : map(lambda x : {"name" : x[0], "type" : x[1]}, COLUMNS) }

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        # This connector cannot have a different schema than its fixed one
        if dataset_schema is not None:
            assert(len(self.get_read_schema()["columns"]) == len(dataset_schema["columns"]))

        logging.info("Starting Github stream with limit = %s" % records_limit)

        query_date = datetime.datetime.now()

        nb = 0

        for issue in itertools.chain(self.repos.get_issues(), self.repos.get_issues(state="closed")):
            issue = GithubIssuesConnector.get_issue(issue)
            issue["query_date"] = str(query_date)

            if records_limit >= 0 and nb >= records_limit:
                return
            yield issue
            nb += 1

            if nb % 100 == 0:
                logging.info("Read %s issues" % nb)

    @staticmethod
    def get_issue(issue):
        ret = {}
        ret["number"] = issue.number
        ret["title"] = issue.title
        ret["state"] = issue.state
        if issue.assignee is not None:
            ret["assignee"] = issue.assignee.login

        def astz(d):
            if d is not None:
                return d.replace(tzinfo=pytz.utc)
            else:
                return None

        ret["created_at"] = astz(issue.created_at)
        ret["updated_at"] = astz(issue.updated_at)
        ret["closed_at"]  = astz(issue.closed_at)
        ret["user"] = (issue.user.login)
        lbls = []
        for label in issue.labels:
            lbls.append(label.name)
        ret["labels"] = json.dumps(lbls)
        milestone = issue.milestone
        if milestone is not None:
            ret["milestone"] = (issue.milestone.title)
        return ret

