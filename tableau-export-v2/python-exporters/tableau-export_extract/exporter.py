from dataiku.exporter import Exporter
from tableau_utils import TDEExport

class TableauExporter(Exporter):
    def __init__(self, config, plugin_config):
        Exporter.__init__(self, config, plugin_config)

    def open_to_file(self, schema, destination_file_path):
        self.e = TDEExport(destination_file_path, schema['columns'])

    def write_row(self, row):
        self.e.insert_array_row(row)

    def close(self):
        self.e.close()
