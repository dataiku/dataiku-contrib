import os
import time
from collections import OrderedDict

import pandas as pd

from dataiku.customformat import Formatter, FormatExtractor


class SASFormatter(Formatter):
    def __init__(self, config, plugin_config):
        Formatter.__init__(self, config, plugin_config)

    def get_output_formatter(self, stream, schema):
        raise NotImplementedError

    def get_format_extractor(self, stream, schema=None):
        return SASFormatExtractor(stream, schema, self.config)


class SASFormatExtractor(FormatExtractor):
    def __init__(self, stream, schema, config):
        FormatExtractor.__init__(self, stream)

        chunksize = int(config.get("chunksize", "10000"))
        sas_format = config.get("sas_format", "sas7bdat")
        encoding = config.get("encoding", "latin_1")
        dump_to_file = config.get("dump_to_file", False)

        self.hasSchema = schema != None

        try:
            # Pre DSS 9.0.1 solution, keep here for backward compatibility. In the future the "try" part can be removed and only the "except" part remain

            from dataiku.base.java_link import LinkedInputStream
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

            read_from = ForwardSeekStream(stream)
        except ImportError:
            read_from = stream

        if dump_to_file:
            dirname, _ = os.path.split(os.path.abspath(__file__))
            fullpath = os.path.join(dirname, 'dumped-%s.sas7bdat' % (time.time()))
            with open(fullpath, 'w+') as of:
                # Reading 500kb data everytime
                for data in iter((lambda: stream.read(500000)), b''):
                    of.write(data)

            read_from = fullpath

        self.iterator = pd.read_sas(read_from,
                                    format=sas_format,
                                    iterator=True,
                                    encoding=encoding,
                                    chunksize=chunksize)

        self.get_chunk()

    def get_chunk(self):
        # Fix for previewing when using DSS < 4.1.X
        if self.hasSchema:
            self.chunk = next(self.iterator).to_dict('records')
        else:
            self.chunk = [OrderedDict(row) for i, row in next(self.iterator).iterrows()]

        self.chunk_nb = 0

    def read_schema(self):
        return [{"name": c.name, "type": "DOUBLE" if c.ctype == 'd' else "STRING"} for c in self.iterator.columns]

    def read_row(self):
        try:
            if self.chunk_nb >= len(self.chunk):
                self.get_chunk()

            self.chunk_nb += 1
            return self.chunk[self.chunk_nb - 1]

        except StopIteration:
            return None
