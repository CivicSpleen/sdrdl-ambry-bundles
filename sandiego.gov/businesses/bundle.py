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
        
        return  True

    def build(self):
        
        self.build_load()
        
        self.build_ck_geocoder()
        
        self.build_dstk_geocoder()
        
        return True
        

    def build_load(self):
        """Load the CSV file of original data"""
        p = self.partitions.new_partition(table='businesses')
        p.clean()
        
        lr = self.init_log_rate(10000)

        good = 0
        bad = 0
        with p.database.inserter() as ins:
            for i, row in enumerate(self.gen_rows()):
                row = dict(row)

                row['business_acct'] = row['business_acct_']

                ins.insert(row)

                if self.run_args.test and i > 100:
                    break
                    
                lr()
                

        return True
        
    
    def build_ck_geocoder(self):
        """Create a crosswalk to CK geocoded addresses, which link to SANDAG data"""
        from ambry.geo.geocoder import Geocoder

        city_subs = {
                    'La Jolla': 'San Diego'
                }

        g = Geocoder(self.library.dep('geocoder').partition, city_subs)
        
        lr = self.init_log_rate(250)
        
        businesses = self.partitions.find(table='businesses')
        
        p = self.partitions.find_or_new(table = 'ck_addresses')
        p.clean()
        
        good = 0
        bad = 0
        
        with p.inserter() as ins:
            
            for i, bus in enumerate(businesses.rows):
        
                row = {
                    'businesses_id' : bus['id']
                }
        
       
                try:
                    # This just lets us know what addresses aren't geocoding. We'll use the faulures
                    # as bad addresses in a geocoder update. 

                    if bus['city']:
                        row['address_id'], result, parsed = g.parse_and_code(bus['address'], 
                                                        city=bus['city'].title(), state = "CA", zip=bus['zip'])
                                                
                        row['parsed_addr'] = "{}, {}, CA {}".format(parsed.text, parsed.locality.city, parsed.locality.zip)

                    if result:
                        row.update(result)
                        row['name'] = ( 
                            row['direction']+' ' if row['direction'] else '' +
                            row['name']+
                            ' '+row['suffix'] if row['suffix'] else ''
                        )
                        row['id'] = None
                        good += 1
                    else:
                        bad += 1

                except Exception as e:
                    self.error("Failed to parse row {}: {} : {} ".format(i, bus['address'], e.message))
                    raise
                  
                lr("Geocode CK: {} good / {} bad ( {}%) of {}".format(good, bad, round(float(good) / float(good+bad) *100,1), good+bad ))
                
                
                ins.insert(row)
                
                if self.run_args.test and i > 500:
                    break
            
            
    def build_dstk_geocoder(self):
        """Geocode with the Data Science Toolkit"""
        from ambry.geo.geocoders import DstkGeocoder
    
        lr = self.init_log_rate(250)
    
        businesses = self.partitions.find(table='businesses')
    
        def address_gen():
            for row in businesses.query("SELECT * FROM businesses"):
                address = "{}, {}, {} {}".format(row['address'], row['city'], row['state'], row['zip'])
                yield (address, row)
    
        dstk_service = self.config.service('dstk')
        
        dstk_gc = DstkGeocoder(dstk_service, address_gen())
    
        p = self.partitions.find_or_new(table = 'dstk_addresses')
        p.clean()
    
        good = 0
        bad = 0
    
        with p.inserter() as ins:
            
            for i, (k, r, inp_row) in enumerate(dstk_gc.geocode()):

                row = {
                    'businesses_id' : inp_row['id']
                }
                
                if r:
                    row.update(dict(r))
                    row['number'] = r.get('street_number', None) 
                    row['name'] = r.get('street_name', None) 
                    row['city'] = r.get('locality', None)
                    row['state'] = r.get('region', None)
                    row['lat'] = r.get('latitude', None)
                    row['lon'] = r.get('longitude', None)
                    row['county'] = r.get('fips_county', None) 
                
                lr("Geocode DSTK")
                
                ins.insert(row)
                
                if self.run_args.test and i > 500:
                    break
                 
    def build_geolink(self):
        
        from ambry.geo.util import find_geo_containment

        def generate_geometries():
            
            blocks = self.library.dep('blocks').partition
            lr = self.init_log_rate(3000)
            
            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,block in enumerate(blocks.query("SELECT  AsText(geometry) AS wkt, id FROM  sws_boundaries")):
                lr('Load rtree')
                
                if self.run_args.test and i > 200:
                    break
                
                yield i,block['id'], block['wkt']
        
        def generate_points():
            p = self.partitions.find(table = 'dstk_addresses')
            #p = self.library.get('sandiego.gov-businesses-orig-dstk_addresses-1.0.3').partition
            for row in p.rows:
                if  row['lon'] and row['lat']:
                    yield row['lon'], row['lat'], row['businesses_id']

        def mark_contains():
            
            p = self.partitions.find_or_new(table='bus_block_cross')
            p.clean()
            
            lr = self.init_log_rate(3000)
            
            with p.inserter() as ins:
                while True:
                    (p,point_obj,geometry, poly_obj) = yield # Get a value back from find_geo_containment
                    
                    d = dict(businesses_id = point_obj, 
                            block_geoid = poly_obj, # New name
                            geoid = poly_obj # Old name, for development
                    )

                    ins.insert(d)
                    lr('Marking point containment')
        
            
        find_geo_containment(generate_geometries(), generate_points(), mark_contains())
        
