'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    @property
    def header(self):
        import csv
        from ambry.orm import Table
        
        fn = self.filesystem.download('active1')
        
        with open(fn) as f:
            r = csv.reader(f)
            return [ Table.mangle_name(c) for c in r.next() ]

    def gen_rows(self, as_dict = True):
        import csv
        from dateutil.parser import parse as parse_date
        
        for k in self.metadata.sources:

            fn = self.filesystem.download(k)

            with open(fn) as f:
                r = csv.reader(f);
                r.next() # Skip the header
                header = self.header # Get our processed header
                    
                for row in r:
                    if as_dict:
                        row = dict(zip(header, row))
                        row['dba_name'] = unicode(row['dba_name'],errors='ignore') # One row has a funny char
                        row['creation_dt'] = parse_date(row['creation_dt'])
                        row['start_dt'] = parse_date(row['start_dt'])
                        row['exp_dt'] = parse_date(row['exp_dt'])
                        
                        yield row
                    else:
                        yield row
        
        
    def meta(self):
        
        self.prepare()
        
        self.schema.update('businesses', self.gen_rows(as_dict=False), n=500, header=self.header, logger=self.init_log_rate())
        
        self.schema.write_schema()
        
        return  False

    def build(self):

        from ambry.geo.geocoder import Geocoder
  
        g = Geocoder(self.library.dep('geocoder').partition)
        
        bus = self.partitions.find(table='businesses')

        p = self.partitions.new_partition(table='businesses')

        p.clean()
        
        lr = self.init_log_rate(1000)

        with p.database.inserter() as ins:
            for i, row in enumerate(self.gen_rows()):
                row = dict(row)
   
                try:
                    # This just lets us know what addresses aren't geocoding. We'll use the faulures
                    # as bad addresses in a geocoder update. 
                    row['address_id'], parsed = g.parse_and_code(row['address'], row['city'].title(), "CA")
                    row['parsed_addr'] = "{}, {}, CA ".format(parsed.text, parsed.locality.city)

                except Exception as e:
                    self.error("Failed to parse row {}: {} : {} ".format(i, row['address'], e.message))

                ins.insert(row)
                lr()
                
                if self.run_args.test and i > 100:
                    break
                

        return True
        


                
