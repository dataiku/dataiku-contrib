from dataiku.connector import Connector
import urllib2 as url2
from BeautifulSoup import UnicodeDammit

class GutenbergConnector(Connector):

    def __init__(self, config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor
        """
        Connector.__init__(self, config)  # pass the parameters to the base class

        # Fetch configuration
        self.mirror = self.config['mirror']
        self.book_id= self.config['book_id']

    def get_read_schema(self):
        """
        Returns the schema for the Gutenberg connector.
        """

        # The Gutenberg connector does not specify a schema,
        # so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.
        """

        url_book = self.mirror
        lid = len(str(self.book_id))
        bid = str(self.book_id)
        print type(lid)

        stopit = 0
        for i in range(lid-1):
            if (bid[i+1] != "-") and (stopit ==0):
                url_book += '/'+bid[i]
            else:
                stopit=1
                bidonly=bid[0:i]
        url_book += '/'+ bidonly  + '/'+ bid + '.txt'

        print url_book
        response = url2.urlopen(url_book)
        raw = response.read()   #.decode('utf8')
        converted = UnicodeDammit(raw)
        raw = converted.unicode
        start_book = raw.find("START OF")
        end_book = raw.rfind('END OF')
        preamb = raw[:start_book]

        author = [ i.split(':')[1].strip() for i in preamb.split("\r\n\r\n") if i.find('Author') != -1][0]
        title = [ i.split(':')[1].strip() for i in preamb.split("\r\n\r\n") if i.find('Title') != -1][0]
        date = [ i.split(':')[1].strip() for i in preamb.split("\r\n\r\n") if i.find('Release Date') != -1][0]
        book_paraph =  raw[start_book:end_book].split("\r\n\r\n")

        print "Book length %s" % len(raw)
        print "N paragraphs:", len(book_paraph)

        for id_p, p in enumerate(book_paraph):
            yield {'id':id_p, 'author': author, 'title': title, 'text': p}
