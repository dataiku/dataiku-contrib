from dataiku.exporter import Exporter
from tableau_utils import TDEExport, upload_tde_file
import tempfile, os

class TableauExporter(Exporter):
    def __init__(self, config, plugin_config):
        Exporter.__init__(self, config, plugin_config)

    def open(self, schema):
        # Fairly ugly. We create and delete a temporary file while retaining its name
        with tempfile.NamedTemporaryFile(prefix="output", suffix=".tde", dir=os.getcwd()) as f:
            self.output_file = f.name
        print "Tmp file: %s" %  self.output_file
        self.e = TDEExport(self.output_file , schema['columns'])

    def write_row(self, row):
        self.e.insert_array_row(row)

    def close(self):
        self.e.close()
        upload_tde_file(self.output_file, self.config)
