'''
Original import of California START tests
'''

from  ambry.bundle import BuildBundle
from ambry.util import memoize

class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def generate_files(self):
        """A Generator that yields the input files. """
        import re
        for year, url in self.metadata.build.sources.items():
                zf = self.filesystem.download(url)
                for fn in self.filesystem.unzip_dir(zf, re.compile(r'.*all.*', re.IGNORECASE)):
                    yield year, fn

    def generate_rows(self):
        import csv
        
        for year, fn in self.generate_files():
            hm = self.header_map(year)
            with open(fn) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield year,row

    def meta_header_map(self):
        '''Build a header map, from the headers of each file to the common headers. This 
        will require manual editing after generation, so do regenerate it needlessly'''
        import csv
        
        header_fn = self.filesystem.path('meta','header_map.csv')
    
        if os.path.exists(header_fn):
            return 
    
        with open(header_fn, 'w') as hfn:
            writer = csv.writer(hfn)
            for year, fn in self.generate(files):
                with open(fn) as f:
                    reader = csv.reader(f)
                    writer.writerow([year]+reader.next())
                
        return True

    def meta_build_schema(self):
        
        self.database.create()
        
        with self.session:
            self.schema.clean
        
            t = self.schema.add_table('star')
            self.schema.add_column(t, 'id',datatype='INTEGER', is_primary_key = True)
            hm = self.header_map(2010)
        
            for k,v in hm.items():
                self.schema.add_column(t,v,description=k, datatype='INTEGER')
        
            def g():
                for i, (year, row) in enumerate(self.generate_rows()):
                    if i > 1000:
                        raise StopIteration
                    hm = self.header_map(year)
                    yield { hm[k]:v for k,v in row.items() }
                      
         
            self.schema.update( "star", g())
        
        self.schema.write_schema()
        
    @memoize
    def header_map(self, year):
        import csv
        
        header_fn = self.filesystem.path('meta','header_map.csv')
        
        with open(header_fn) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row['key_year']) == str(year):
                    del row['key_year']
                    return  {value: key for key, value in row.items()}
                    
    def build(self):

        n = self.run_args.multi
        
        if n  == 1:
            for year, fn in self.generate_files():
                self.build_year(year, fn)
        else:
            args = [ (year, fn) for year, fn in self.generate_files()]

            self.run_mp(self.build_year, args)

            
        return True
        
    def build_year(self, year, fn):
        import csv
        
        lr = self.init_log_rate(20000)
        
        p = self.partitions.find_or_new(table='star', time=year)
        p.clean()
        
        i = 0
        with p.inserter() as ins:   
            hm = self.header_map(year)
            self.log("Processing: {}, {}".format(year, fn))
            with open(fn) as f:
                reader = csv.DictReader(f)
                for in_row in reader:
                    row = { hm[k]:v if v != '*' else None for k,v in in_row.items() }
                    lr() #print row
                    i += 1
                 
                    row['cds'] = row['county'] + row['district'] + row['school']
        
                    cast_errors = ins.insert(row)
                    
                    if cast_errors:
                        print cast_errors
                        
                    if self.run_args.test and i >= 10000:
                        break
                
        

                    
                    
                    
                    
                    
                
                    
        