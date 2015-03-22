'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def generate_rows(self):
        from xlrd import open_workbook, xldate_as_tuple
        from dateutil.parser import parse
        from datetime import datetime
        import re
        
        header = self.metadata.build.header
        
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)
            
            wb = open_workbook(f)

            s = wb.sheets()[0]

            in_body = False

            for i,row in enumerate(range(s.nrows)):
                values = []
                for col in range(s.ncols):
                    values.append(s.cell(row,col).value)
                    
                if in_body:
                    d =  dict(zip(header, values))
                    
                    d['approval_msg'] = ''
                    
                    try:
                        d['received'] = datetime(*xldate_as_tuple(d['received'], wb.datemode))
                    except ValueError: # The cell is a string, not a float
                        d['received']= parse(d['received'], yearfirst = False, dayfirst = False)
                        
                    try:
                        d['approved'] = datetime(*xldate_as_tuple(d['approved'], wb.datemode))
                    except ValueError: # The cell is a string, not a float
                        msg = []
                        approved = d['approved']
                        d['approved'] = None
                        
                        for part in re.sub(r'[^\w\d\s\/]','',approved).split(' '):
                            try:
                                # Get the words that may be a date
                                d['approved'] = parse(part, yearfirst = False, dayfirst = False)
                            except ValueError as e:
                                msg.append(part)
                            except TypeError:
                                msg.append(part)
                                
                        d['approval_msg'] = ' '.join(msg).title()


                    if d['approved']:
                        d['approved_year'] = d['approved'].year
                        d['approved_month'] = d['approved'].month
                        d['approved'] = d['approved'].date()
                        
                    if d['received']:
                        d['received_year'] = d['received'].year
                        d['received_month'] = d['received'].month
                        d['received'] = d['received'].date()
                    
                        
                    yield d
                    

                if 'Permit #' in values[0]: # The header is not on the first line. 
                    in_body = True
        
    def meta(self):
        
        self.database.create() # We need to have the metadata database to be able to create the schema. 

        self.schema.update('permits', self.generate_rows(), n=20000)
        

    def build(self):
        
        from dateutil.parser import parse
        import pprint 
        p = self.partitions.new_partition(table='permits')
        
        with p.inserter() as ins:
            for i, d in enumerate(self.generate_rows()):

                errors = ins.insert(d)

        return True
        
            
        

