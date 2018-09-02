from dataiku.exporter import Exporter
from tableau_utils import TableauExport

class TableauHyperExporter(Exporter):
    def __init__(self, config, plugin_config):
        Exporter.__init__(self, config, plugin_config)

    def open_to_file(self, schema, destination_file_path):
        self.e = TableauExport(destination_file_path, schema['columns'])

    def write_row(self, row):
        self.e.insert_array_row(row)

    def close(self):
        print "Hyper: Exported %d rows" % self.e.nrows
        self.e.close()