'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_make_headers(self):
        from xlrd import open_workbook
        import re
        
        try:
            headers = self.filesystem.read_yaml('meta','headers.yaml')
        except IOError:
            headers = {}
            
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)
            
            
            for fn in self.filesystem.unzip_dir(f):
            
                wb = open_workbook(fn)

                s = wb.sheets()[0]

                for i,row in enumerate(range(s.nrows)):
                    values = []
                    for col in range(s.ncols):
                        values.append(s.cell(row,col).value)

                    if values[0].startswith('MSA'):
                        for k in values:
                            if k not in headers:
                                headers[k] = ''
                        break
                        
        self.filesystem.write_yaml(headers, 'meta','headers.yaml')

        return True

    def meta_make_schema(self):
        
        lr = self.init_log_rate(10000)
        
        self.database.create()
        
        self.schema.update('empwages', self.generate_rows(), n=20000, logger=lr)

    def meta(self):
        
        self.meta_make_headers()

        self.meta_make_schema()
        
        
        return True

    def generate_rows(self):
        from xlrd import open_workbook
        import re
        headers = set()

        headers = self.filesystem.read_yaml('meta','headers.yaml')

        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)

            year, quarter = re.match(r'(\d{4})Q(\d)', k).groups()

            for fn in self.filesystem.unzip_dir(f):
            
                in_table = False
                header = None
            
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
                        

                    elif values[0].startswith('MSA'):
                        in_table = True
                        header = [ headers[v] for v in values]
                        
    def build(self):
        from ambry.database.inserter import CodeCastErrorHandler
        lr = self.init_log_rate(10000)
        
        p = self.partitions.new_partition(table='empwages')

        with p.inserter(cast_error_handler = CodeCastErrorHandler) as ins:
            for i, d in enumerate(self.generate_rows()):
                errors = ins.insert(d)
                lr('Loading')
        
        return True
