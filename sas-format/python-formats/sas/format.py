from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor
from dataiku.base.java_link import LinkedInputStream
from collections import OrderedDict
from distutils.version import LooseVersion

import pandas as pd
import json, base64
import types

class SASFormatter(Formatter):
    def __init__(self, config, plugin_config):
        Formatter.__init__(self, config, plugin_config)
        
    def get_output_formatter(self, stream, schema):
        raise NotImplementedError
        
    def get_format_extractor(self, stream, schema=None):
        return SASFormatExtractor(stream, schema, self.config)    

    
# Fix for the stream class provided by DSS
# Seek could be disabled by a one-liner like the following one but read_sas may seek forward
# self.stream.seek = types.MethodType(lambda self, _: False, self.stream)
class ForwardSeekStream(LinkedInputStream):
    def __init__(self, stream):
        LinkedInputStream.__init__(self, stream.link)
        self.stream = stream
        self.size_read = 0
    
    def readinto(self, b):
        res = LinkedInputStream.readinto(self, b)
        self.size_read += res
        return res

    def seek(self, seek, whence=0):    
        to_read = seek if whence == 1 else seek - self.size_read
                
        if to_read < 0: 
            raise IOError("Only forward seeking is supported")   
        elif to_read > 0:
            self.read(to_read)


class SASFormatExtractor(FormatExtractor):
    def __init__(self, stream, schema, config):
        FormatExtractor.__init__(self, stream)
        
        chunksize = int(config.get("chunksize", "10000"))
        
        self.iterator = pd.read_sas(ForwardSeekStream(stream), format="sas7bdat", iterator=True, chunksize=chunksize)
        self.is_pandas_021 = LooseVersion(pd.__version__) >= LooseVersion('0.21.0')
        self.get_ordered_chunk()

    # OrderedDict is needed to preserve the column order, but the
    # `into` parameter was only introduced in Pandas 0.21.0
    def get_ordered_chunk(self):
        if (self.is_pandas_021):
            self.chunk = next(self.iterator).to_dict('records', into=OrderedDict)
        else:
            self.chunk = [OrderedDict(row) for i, row in next(self.iterator).iterrows()]
        
        self.chunk_nb = 0
        
    def read_schema(self):
        return [{"name": c.name, "type": "DOUBLE" if c.ctype == 'd' else "STRING"} for c in self.iterator.columns]
    
    def read_row(self):
        try:
            if self.chunk_nb >= len(self.chunk): 
                self.get_ordered_chunk()
                
            self.chunk_nb += 1
            return self.chunk[self.chunk_nb - 1]

        except StopIteration:
            return None
        