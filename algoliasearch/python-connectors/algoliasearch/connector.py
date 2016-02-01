import json, datetime, itertools
from algoliasearch import algoliasearch
from dataiku.connector import Connector, CustomDatasetWriter
import logging

def _get_facet_filters(dataset_partitioning, partition_id):
    facetFilters = []
    idx = 0
    id_chunks = partition_id.split("|")
    for dim in dataset_partitioning["dimensions"]:
        facetFilters.append(dim["name"] + ":" + id_chunks[idx])
        idx += 1
    return facetFilters


class AlgoliaSearchConnector(Connector):
    def __init__(self, config):
        Connector.__init__(self, config)

    def get_read_schema(self):
        # The Algolia connector does not provide a schema, since
        # the index is schemaless
        return None

    def _get_index(self):
        client = algoliasearch.Client(self.config["applicationId"], self.config["apiKey"])
        index = client.init_index(self.config["index"])
        return index

    def _get_base_search_settings(self):
        if len(self.config.get("searchSettings", "")) > 0:
            print "Loading settings : -%s-" % self.config["searchSettings"]
            return json.loads(self.config["searchSettings"])
        else:
            return {}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        print "Algolia connector: generating with partitioning=%s partition=%s limit=%s" % (dataset_partitioning, partition_id, records_limit)

        index = self._get_index()

        search_settings = self._get_base_search_settings()
        search_settings["attributesToHighlight"] = []

        if records_limit >= 0:
            search_settings["hitsPerPage"] = records_limit

        if dataset_partitioning is not None:
            facetFilters = _get_facet_filters(dataset_partitioning, partition_id)

            print "Searching with facets: %s" % ",".join(facetFilters)
            if search_settings.get("facetFilters", None) is not None:
                search_settings["facetFilters"] = search_settings["facetFilters"] + "," +",".join(facetFilters)
            else:
                search_settings["facetFilters"] = facetFilters

        print "Final settings : %s" % search_settings

        res = index.search(self.config.get("searchQuery", ""), search_settings)

        for hit in res["hits"]:
            if "_highlightResult" in hit:
                del hit["_highlightResult"]

            yield hit

    def list_partitions(self, dataset_partitioning):
        assert dataset_partitioning is not None

        facets = [dim["name"] for dim in dataset_partitioning["dimensions"]]

        search_settings = self._get_base_search_settings()
        search_settings["attributesToRetrieve"] = []
        search_settings["attributesToHighlight"] = []
        search_settings["facets"] = facets

        index = self._get_index()
        res = index.search(self.config.get("searchQuery", ""), search_settings)

        vals =[]

        for dim in dataset_partitioning["dimensions"]:
            facet = res["facets"][dim["name"]]
            vals.append(facet.keys())

        ret = []
        import itertools
        for element in itertools.product(*vals):
            ret.append("|".join(element))
        print ret
        return ret

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):

        return AlgoliaSearchConnectorWriter(self.config, self, self._get_index(),
                    dataset_schema, dataset_partitioning, partition_id)

class AlgoliaSearchConnectorWriter(CustomDatasetWriter):
    def __init__(self, config, parent, index, dataset_schema, dataset_partitioning, partition_id):
        CustomDatasetWriter.__init__(self)
        self.parent = parent
        self.config = config
        self.index = index
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id

        self.buffer = []

        if dataset_partitioning is not None:
            # Clear by query:
            search_settings = self.parent._get_base_search_settings()
            search_settings["facetFilters"] =  _get_facet_filters(dataset_partitioning, partition_id)
            self.index.delete_by_query("*", search_settings)
        else:
            self.index.clear_index()

    def write_row(self, row):
        logging.info("Algolia Write dataset_partitioning=%s" % self.dataset_partitioning)
        obj = {}
        for (col, val) in zip(self.dataset_schema["columns"], row):
            #logging.info("Write %s for %s" % (val, col))
            if len(unicode(val)) > 5000:
                val = unicode(val)[0:5000]
            if col['type'] in ['tinyint', 'smallint', 'int', 'bigint']:
                try:
                    val = int(val)
                except:
                    print "Warning: could not parse as int:", val
            if col['type'] in ['array', 'object', 'map']:
                try:
                    val = json.loads(val)
                except:
                    print "Warning: could not parse:", val
            obj[col["name"]] = val
            if col["name"] =="id":
                logging.info("Set ObjectID")
                obj["objectID"] = val

        if self.dataset_partitioning is not None:
            id_chunks = self.partition_id.split("|")
            idx = 0
            for dim in self.dataset_partitioning["dimensions"]:
                obj[dim["name"]] = id_chunks[idx]
                logging.info("Forcing partitioning dim: %s=%s" % (dim["name"], id_chunks[idx]))
                idx += 1

        logging.info("Final obj: %s" % obj)
        self.buffer.append(obj)

        if len(self.buffer) > 50:
            self.flush()

    def flush(self):
        logging.info("Flushing Algolia buffer")
        self.index.save_objects(self.buffer)
        self.buffer = []

    def close(self):
        self.flush()
        logging.info("Closing Algolia")
