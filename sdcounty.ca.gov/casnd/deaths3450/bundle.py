'''
Process the San Diego Count 3-4-50 deaths, with a lot of value substitution 
in the caster
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    #
    # The following  are custom caster types. They are linked 
    # to fields in the d_caster column of the schema. These can be derived
    # functions, or derived from types. 
    # 

    @staticmethod
    def lt5(v):
        if v == "<5":
            v = 0
    
        return int(v)

    class na(float):
        ''' Handle n/a and % entries'''
        def __new__(self, v):
            
            v = v.strip("%")
            if not v or v == 'n/a':
                return None
            
            return float.__new__(self, v)

    class percent(float):
    
        def __new__(self, v):
            '''Handle percentages and n/a'''
            v = v.strip('%')
            if not v:
                v = 0
                
            if v == 'n/a':
                return None
              
            if v:
                return float.__new__(self, float(v)/100.0)

    def meta(self):
        '''Create the schema from a YAML mapping file'''
        import unicodecsv as csv

        # We will be creating the bundle database to create the schema
        # then discarding it before the  build phase. 
        self.database.create()

        with self.session: 
            t = self.schema.add_table('deaths3450')
            
            self.schema.add_column(t, 'id', datatype = 'integer',
                                    is_primary_key = True)

            for r in self.filesystem.read_yaml('meta','header.yaml'):
                self.schema.add_column(t, r[1], datatype = 'integer')
        
        self.schema.write_schema()
        
        return True

    def build(self):
        import unicodecsv as csv
        
        header = self.schema.table('deaths3450').header

        # 'deaths' is defined in the configuration build.sources.deaths
        fn = self.filesystem.download('deaths')

        # Finding and clearing, rather than creating, is used to make multiple
        # runs in development easier
        p = self.partitions.find_or_new(table = 'deaths3450')
        p.clean()

        with p.inserter() as ins:
            with open(fn) as f:
                reader = csv.reader(f)
                reader.next() # Skip the file header
                for row in reader:
                    # Add [None] to provide a value for the primary key field we
                    # added when constructing the schema
                  
                    drow = dict(zip(header, [None]+row))

                    drow['area'] = drow['area'].title()

                    errors = ins.insert(drow)
                    
                    if errors:
                        self.error("Failed to insert with error {}\n row: {}"
                                    .format(errors, drow))

            
        return True
        
        
        
