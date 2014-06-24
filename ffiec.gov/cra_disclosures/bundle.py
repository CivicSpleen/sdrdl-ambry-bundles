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
        import re
        
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
                    out_row['Field'] = re.sub(r'\(Originations (?:and|\&) Purchases\)', '', out_row['Field'])
                    out_row['Field'] = out_row['Field'].strip('.')
                    
            yield out_row
        
    def meta_generate_tables(self, year):
        
        table_name = None
        table = []
        for row in self.meta_generate_dd_rows(year):

            if row['Start'] == '1': # The start of a table. 
                if table:
                    yield table_name, table
                    table = []
            
                table_name = row['Comments'].strip().split(' ')[-1].lower().replace('-','_')
                
            row['Table'] = table_name
            
            table.append(row)

        yield table_name, table
        
    def make_column_name(self, field):
    
    
        x = []
        
        field = field.lower()

        if field.startswith('number'):
            x.append('num')
        elif field.startswith('total'):
            x.append('tot')
        else:
            return x
            
        if  'originated' in field:
            x.append('orig')
        elif 'purchased' in field:
            x.append('purch')
        
        if 'business' in field:
            x.append('bus')
        elif 'farm' in field:
            x.append('farm')   
        
        if 'affiliate' in field:
            x.append('al')
        
        if '> $100,000' in field:
            x.append('gt100k')
            
        if '> $250,000' in field:
            x.append('gt250k')
            
        if '> $500,000' in field:
            x.append('gt500k')
        
        if '< $100,000' in field:
            x.append('lt100k')
            
        if '< $250,000' in field:
            x.append('lt250k')
            
        if '< $500,000' in field:
            x.append('lt500k')
        
        if '< $1 million' in field or '< $1,000,000' in field:
            x.append('lt1m')

            
        return x
    

    def meta(self):
        import re

        tables = {}
        try:
            field_names = self.filesystem.read_yaml('meta','field_map.yaml')
        except IOError:
            field_names = None
            
        if not field_names:
            field_names = {}

        tables = dict()

        for year in range(1996, 2013):
            for table_name, table in self.meta_generate_tables(year):
                print '------- {} {} ---------'.format(year, table_name)

                for i,row in enumerate(table):
                    #print row['Start'], row['End'], row['Field']
                    fn = re.sub(r'^\d+[\.\s]*','',row['Field'].strip())
                    if not fn in field_names:
                        field_names[fn] = '_'.join(self.make_column_name(fn))
                        
                    # default dict cant be stored in Yaml with out special converter class
                    if not table_name in tables:
                        tables[table_name] = {}
                        
                    if i not in tables[table_name]:
                        tables[table_name][i] = []
              
                    if fn.lower() not in tables[table_name][i]:
                        tables[table_name][i].append(fn.lower())

                     

        self.filesystem.write_yaml(field_names, 'meta','field_map.yaml')
                    
        self.filesystem.write_yaml(tables, 'meta','column_names_by_pos.yaml')

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

