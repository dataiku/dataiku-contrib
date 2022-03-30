from dataiku.connector import Connector
import logging
import json
import datetime
import requests

class GithubPullRequestsConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        access_token = config["personal_access_token_credentials"]["personal_access_token_credentials"]
        self.current_number_of_fetched_prs = 0
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "token {}".format(access_token)
        }

        # Doc: https://docs.github.com/en/rest/reference/pulls
        pr_url_template = "https://api.github.com/repos/{owner}/{repository}/pulls"
        self.pr_repository_urls = \
            [pr_url_template.format(owner="dataiku", repository=repository) for repository in (config["repositories"])]

        self.PER_PAGE = 100
        self.PR_URL_WITH_PAGE_ID_TEMPLATE = "{pr_repository_url}?per_page={per_page};page={page_id}"

    def fetch_pull_requests(self, pr_repository_url, records_limit, current_number_of_fetched_prs=0, page_id=1):
        pr_repository_url_with_page_number = self.PR_URL_WITH_PAGE_ID_TEMPLATE.format(
            pr_repository_url=pr_repository_url, page_id=page_id, per_page=self.PER_PAGE
        )
        logging.info("Fetching PR requests from {}".format(pr_repository_url_with_page_number))
        r = requests.get(pr_repository_url_with_page_number, headers=self.headers)
        r.raise_for_status()
        try:
            pull_requests = json.loads(r.content)
            number_of_fetched_prs = len(pull_requests)
            current_number_of_fetched_prs += number_of_fetched_prs
            if 0 <= records_limit <= current_number_of_fetched_prs:
                logging.info(
                    "Reached records_limit (limit={}): {}, stopping fetch of new records for this repository.".format(
                        records_limit, current_number_of_fetched_prs
                    )
                )
                return pull_requests
            if number_of_fetched_prs is self.PER_PAGE:
                pull_requests = \
                    pull_requests + self.fetch_pull_requests(
                        pr_repository_url, records_limit, current_number_of_fetched_prs, page_id+1
                    )
            return pull_requests
        except Exception:
            logging.info("Could not parse json from request content:\n" + r.content)
            raise

    def get_read_schema(self):
        # Let DSS infer the schema from the columns returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):
        query_date = datetime.datetime.now()

        pull_requests = []
        for pr_repository_url in self.pr_repository_urls:
            pull_requests_for_repository = self.fetch_pull_requests(pr_repository_url, records_limit)
            number_of_fetched_prs = len(pull_requests_for_repository)
            pull_requests = pull_requests + pull_requests_for_repository
            current_number_of_fetched_prs = len(pull_requests)
            logging.info("Fetched {} pull requests for {} (current total of fetched PR: {})".format(
                number_of_fetched_prs, pr_repository_url, current_number_of_fetched_prs)
            )
            if 0 <= records_limit <= current_number_of_fetched_prs:
                logging.info(
                    "Reached records_limit (limit={}): {}, stopping fetch of new records from any repositories.".format(
                        records_limit, current_number_of_fetched_prs
                    )
                )
                break

        nb = 0
        for pull_request in pull_requests:
            if 0 <= records_limit <= nb:
                logging.info("Reached records_limit ({}), stopping inserting new records in dataset.".format(
                    records_limit
                ))
                return

            pull_request["query_date"] = query_date
            yield pull_request
            nb += 1
