'''
Wages per occupation and time in California, from the BLS OES. 
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_make_headers(self):
        """Create the headers map, from the headers in each file to column names that are common
        to all files. """
        
        # xldr is installed automatically by ambry, based on the contents if the contents of 
        # meta/build.yaml:build.requirements dictionary. The keys are the name of the packages, and
        # the values are what gets passed into pip
        from xlrd import open_workbook
        import re
        
        # The headers.yaml file is hand edited, so we want to update, not recreate it. 
        try:
            headers = self.filesystem.read_yaml('meta','headers.yaml')
        except IOError:
            headers = {}
            
        # The sources metadata item is in meta/build.yaml:sources. The keys in this dict
        # can be used in the download() function to download the referened url. 
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)
            for fn in self.filesystem.unzip_dir(f):
            
                wb = open_workbook(fn)

                s = wb.sheets()[0]

                for i,row in enumerate(range(s.nrows)):
                    values = []
                    for col in range(s.ncols):
                        values.append(s.cell(row,col).value)

                    if values[0].startswith('MSA'): # The header is not on the first line. 
                        for k in values:
                            if k not in headers:
                                headers[k] = ''
                        break
                        
        self.filesystem.write_yaml(headers, 'meta','headers.yaml')

        return True

    def meta_make_schema(self):
        """Generate the schema. 
        
        WARNING: This will get a lot of fields wrong. Many of what should be integer columns have non
        integer codes, so the update() will intuit these are VARCHAR columns. You'll have to changes these
        manually after running the meta phase. 
        """
        lr = self.init_log_rate(10000)
        
        self.database.create() # We need to have the metadata database to be able to create the schema. 
        
        # This will load the first 20,000 records and try to figure out what each of the
        # column types are. When it is done, it will write the schema back into meta/schema.csv. 
        self.schema.update('empwages', self.generate_rows(), n=20000, logger=lr)
        
        

    # The Meta Phase
    # The meta phase is intended to be run in development, producing files that get checked into git. Often, the
    # meta phase produces files that require hand editing
    def meta(self):
        
        self.meta_make_headers()

        self.meta_make_schema()
        
        
        return True

    def generate_rows(self):
        """Generate rows from the input files, downloadig them if necessary. Also converts
        header lines and adds the year and quarter values. """
        # xldr is installed autmatically by ambry, based on the contents if the contents of 
        # meta/build.yaml:build.requirements dictionary. The keys are the name of the packages, and
        # the values are what gets passed into pip
        from xlrd import open_workbook
        import re
        
        headers = set()

        # meta/headers.yaml is created in the meta phase, then hand edited. 
        headers = self.filesystem.read_yaml('meta','headers.yaml')

        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)

            # The files don't have the year and quarter as data, and for 2010, the filename
            # doesn't reference the year, so we have to get the year from our source keys. 
            year, quarter = re.match(r'(\d{4})Q(\d)', k).groups()
            year = int(year)
            quarter = int(quarter)

            for fn in self.filesystem.unzip_dir(f): # Unzip the file and iterate over the files inside. 
            
                in_table = False
                header = None
            
                if year == 2010 and '2011' in fn:
                    # Inexplicably, there are 2011 files in the 2010 zip file, so 
                    # exclude them
                    print "In {}, skipping {}".format(year, fn)
                    continue
                    
            
                wb = open_workbook(fn)

                s = wb.sheets()[0]

                for i,row in enumerate(range(s.nrows)):
                    values = []
                    for col in range(s.ncols):
                        values.append(s.cell(row,col).value)

                    if in_table:
                        d =  dict(zip(header, values))
                        
                        d['year'] = year
                        d['quarter'] = quarter
                        
                        if d['soc']:
                            yield d
                        
                    # Don't start yielding until after the real header line, which isn't the 
                    # first in the file. 
                    elif values[0].startswith('MSA'):
                        in_table = True
                        header = [ headers[v] for v in values]
             
                    
    def build(self):
        
        # The CodeCastErrorHandler catches all conversion errors and turns them into 
        # _code field entries. So, if there is a '(3)' as a flag in the wages column ( an integer), it gets
        # stored in the varchar wages_code column. 
        from ambry.database.inserter import CodeCastErrorHandler
        
        # The rate logger will print a message every 10,000 calls. You can also use print_rate=<sec> to 
        # print a message every few second, regardless of the rate. 
        lr = self.init_log_rate(10000)
        
        # We have to create a partition to store the data. Every partition must reference at least one table, 
        # but can also specify time, space, grain, segment and other variant dimensions. 
        p = self.partitions.new_partition(table='empwages')
        
        # A common idiom for partitions is to use find_or_new() plus p.clean(), so that in development, 
        # build() can be re-run without re-creating the schema with --clean
        
        # Get an inserter so we can load the partition. 
        with p.inserter(cast_error_handler = CodeCastErrorHandler) as ins:
            for i, d in enumerate(self.generate_rows()):
                
                # The errors is a dict of the keys and values of the column values that didn't convert property. 
                # It holds the same data that the CodeCastErrorHandler uses to build the code columns. 
                errors = ins.insert(d)
                
                lr('Loading {}'.format(d['year']))
        
        return True # Be sure to return True on success, or the build isn't marked as complete. 
        
    # This is a test function. You can run it with: bambry run print_files 
    def print_files(self):
        import re 
        
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)

            year, quarter = re.match(r'(\d{4})Q(\d)', k).groups()

            for fn in self.filesystem.unzip_dir(f):
                print year, quarter, fn
        
