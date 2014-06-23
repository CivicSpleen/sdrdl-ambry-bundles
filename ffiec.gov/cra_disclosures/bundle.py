'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_generate_dd_rows(self, year):
        '''Read the file and combines multiple comment rows into single field entry rows'''
        import csv
        
        fn = self.filesystem.path('meta','disclosure_datadicts','datadict-disclosures-{}.csv'.format(year))
        
        header  = 'Field,Start,End,Length,Type,Comments'.split(',')
        
        out_row = None
        with open(fn) as f:
            reader = csv.reader(f)

            for r in reader:
                
                if r[0].strip() == 'Field': # Header line, internal to the CSV file, due to PDF conversion
                    continue
                    
                r = dict(zip(header, r))
               
                
                if r['Field'].strip() and r['Start'].strip():
                    if out_row:
                        yield out_row

                    r['Field'] = ' '.join([ x.strip() for x in r['Field'].split()])

                    out_row = r
                   

                else:
                    out_row['Comments'] += ( '\n' + r['Comments'])
                    out_row['Field'] +=  ( ' ' + ' '.join([ x.strip() for x in r['Field'].split()]))
        
            yield out_row
        
    def meta_generate_tables(self, year):
        
        table_name = None
        table = []
        for row in self.meta_generate_dd_rows(year):

            if row['Start'] == '1': # The start of a table. 
                if table:
                    yield table_name, table
            
                table_name = row['Comments'].strip().split(' ')[-1].lower().replace('-','_')
                
            row['Table'] = table_name
            
            table.append(row)

        yield table_name, table
        

    def meta(self):
        import re, collections
        
        
        tables = {}
        try:
            field_names = self.filesystem.read_yaml('meta','field_map.yaml')
        except:
            raise 
            
        if not field_names:
            field_names = {}

        tables = colections.defaultdict(lambda: defaultdict(list))

        for year in range(1996, 2013):
            for table_name, table in self.meta_generate_tables(year):
                #print '------- {} {} ---------'.format(year, table_name)
                for i,row in enumerate(table):
                    #print row['Start'], row['End'], row['Field']
                    fn = re.sub(r'^\d+[\.\s]*','',row['Field'].strip())
                    if not fn in field_names:
                        field_names[fn] = ''
                        
                    tables[table_name][i].append(fn)
                        
                    
                    
        self.filesystem.write_yaml(field_names, 'meta','field_map.yaml')
                    


    def build(self):
        import uuid
        import random

        p = self.partitions.new_partition(table='example1')

        p.query('DELETE FROM example1')

        lr = self.init_log_rate(100)

        with p.database.inserter() as ins:
            for i in range(1000):
                row = {}
                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0,100)
                row['float'] = random.random()*100

                ins.insert(row)
                lr()

        return True

