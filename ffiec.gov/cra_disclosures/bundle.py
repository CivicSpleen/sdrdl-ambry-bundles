'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_generate_dd_rows(self, year):
        '''Read the file and combine multiple comment rows into single field entry rows.
        Also munges the Field name'''
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
        """Generates tables for a year file, where each table is an array olf column entries."""
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
        """Create a mnemonic column name for the purchase and original value fields, so the
        name can be stored in the colum name mape for easier editing.  """
    
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
    

    def meta_field_map(self):
        """Write two field map files, one for review, and one ( field_map.yaml) for hand editing
        to set shorter column names. """
        import re

        tables = {}
        try:
            field_names = self.filesystem.read_yaml('meta','field_map.yaml')
        except IOError:
            field_names = None
            
        if not field_names:
            field_names = {}

        tables = dict()

        for year in range(*self.metadata.build.years):
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

    def meta_make_regexes(self):
        """ Create a YAML file of regular expressions for parsing each of the tables in each year file. There
        is a lot of redundancy between the years, but there are a few differences, so it is more reliable
        to enumerate all of the regexes. """
        import re 
        
        field_names = self.filesystem.read_yaml('meta','field_map.yaml')

        regexes  = {}

        for year in range(*self.metadata.build.years):
        
            assert year not in regexes
            
            regexes[year] = {}
        
            for table_name, table in self.meta_generate_tables(year):
                self.log('{} {}'.format(year, table_name))

                pos = 0
                regex = ''
                header = []
                
                for i,row in enumerate(table):
                    fn = re.sub(r'^\d+[\.\s]*','',row['Field'].strip())
                    col_name = field_names[fn]
                    
                    size = int(row['Length'])
                    
                    # A lot of the filler sizes are wrong, and they are all blanks anyway, 
                    # so we'll just strip them off
                    if col_name == 'filler':
                        continue

                    
                    pos += size
                
                    regex += "(?P<{name}>.{{{size}}})".format(size=size, name=col_name)
                    header.append(col_name)

                
                regexes[year][table_name] = regex
                 
                 
        self.filesystem.write_yaml(regexes, 'meta','table_regexes.yaml')        

    def meta_gen_urls(self):
        """Expand a URL template into source vaules in the build metadata. """
         
        templ = self.metadata.build.url_template

        
        for year in range(*self.metadata.build.years):
            self.metadata.sources[year].url=templ.format(year%100)
        
        
        self.metadata.write_to_dir(write_all=True)

    def meta_make_schema(self):
        import re
        
        self.database.create()
        
        field_names = self.filesystem.read_yaml('meta','field_map.yaml')
        
        for year in range(*self.metadata.build.years):
            for table_name, table in self.meta_generate_tables(year):
                print '------- {} {} ---------'.format(year, table_name)

                t = self.schema.add_table(table_name);
                self.schema.add_column(t, 'id', datatype='integer', is_primary_key = True)

                for i,row in enumerate(table):
                    fn = re.sub(r'^\d+[\.\s]*','',row['Field'].strip())

                    if row['Type'].strip() == 'N':
                        datatype = 'integer'
                    elif row['Type'].strip() == 'AN':
                        datatype = 'varchar'
                    else:
                        self.log("Bad row?: {}".format(row))
                        raise ValueError("Unknown data type code: '{}'".format(row['Type']))

                    self.schema.add_column(t, field_names[fn], datatype=datatype, width = row['Length'],
                                        description=row['Comments'], data={'orig_field':row['Field']})
        self.schema.write_schema()
        
    def meta(self):

        self.meta_gen_urls()
        
        self.meta_make_regexes()
        
        self.meta_make_schema()
        
        return True


    def build(self):
        import re
        regexes_years = self.filesystem.read_yaml('meta','table_regexes.yaml')        
        
        rm = {}
        
        for year, regexes in regexes_years.items():
            for table, regex in regexes.items():
            
                rm[(year, table)] = re.compile(regex)
    
        
        inserters = {}

        # You'd think we could just iterate over the self.schema.tables directly, 
        # but there is a problem with Sqlalchemy disconnecting the table from the session, 
        # probably due to the partitions.find_or_new destroying the session. This issue
        # crops up a lot. 
        table_names = [t.name for t in self.schema.tables]

        for table_name in table_names:
            self.log("Creating inserter: {}".format(table_name))
            p = self.partitions.find_or_new(table=table_name)
            ins = p.inserter()
            ins.__enter__()
            inserters[table_name] = (p, ins)
            
            lr = self.init_log_rate(N=30000)
        
        for year in range(*self.metadata.build.years):
            
            regexes = regexes_years[year]
            
            z_fn = self.filesystem.download(year)
            fn = self.filesystem.unzip(z_fn)
            
            last_table_id = None
            ins = None
            p = None
            n = 0

    
            with open (fn) as f:
                for i, line in enumerate(iter(f)):
                    
                    line = line.strip()
                    
                    table_id = line[:4].lower().replace('-','_')
                    
                    if last_table_id != table_id:
                        last_table_id = table_id
                        try:
                            ins = inserters[table_id][1]
                        except KeyError:
                            # Some of the files have bad endings. 
                            self.error("Failed to get table_id on line {}, year {}, file {}: '{}'"
                                      .format(i, year, fn, line))
                            continue
                    
                    regex = rm[(year, table_id)]
                    
                    if table_id == 'd5_0':
                        if year == 1996 and len(line) == 34:
                            line += ( '0' * 14 )
                        elif year == 1997 and len(line) == 35:
                            line += ( '0' * 14 )
                        elif year == 1998 and len(line) == 36:
                            line += ( '0' * 14 )
                    
                    
                    
                    m = regex.match(line)
                    
                    if not m:
                        self.error("Failed to match regex to line {}, year {}, file {}, len {}: '{}'"
                                  .format(i, year, fn, len(line), line))
                        continue
                    
                    ins.insert(m.groupdict())
                    
                    lr("{}: {}".format(year, table_id))
                    
                
        for table_name, (p, ins) in inserters.items():
            ins.__exit__(None,None,None)
            
                
        return True
                    
                    
                    
                    
                    
            
            
        
        
            
            
            
        
        
        
        
        

