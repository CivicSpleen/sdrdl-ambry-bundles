'''

'''

from  ambry.bundle.loader import CsvBundle
 
class Bundle(CsvBundle):
    ''' '''
    
    def generate_agencies(self):
        """Load the agency list from the web, and yield geocoded address records"""
        import csv
        from ambry.geo.geocoders import DstkGeocoder
        from collections import defaultdict

        def address_gen():
            
            for row in self.partitions.find(table='sdfb_partners').rows:

                yield ("{} {}, CA {}".format(row['addr1'].decode('ascii','ignore'), row['city'], row['zip']),
                            (row['agencyref'].strip(), None, row['agencyname'].strip()))
            
            for row in self.partitions.find(table='agency_list').rows:
                
                yield (row['address'].decode('ascii','ignore'), 
                      (row['agency_id'], row['site_id'], row['name'].strip()))



        dstk_gc = DstkGeocoder(self.config.service('dstk'), address_gen())

        header = 'agency_id site_id name orig_address geocoded_address city lat lon'.split()

        for i, (k, r, o) in enumerate(dstk_gc.geocode()):
            
            row = [o[0],o[1],o[2],k]
            
            if r:
                row += [r['street_address'], r['locality'], r['latitude'], r['longitude']]
                
            yield dict(
                i = i,
                address = k, 
                geocoded = r,
                row = dict(zip(header, row ))
            )
        
    def build(self):
        
        self.build_from_source('sdfb_partners')
        self.build_from_source('agency_list')
        
        self.build_agencies()
        
        return True
        
    def build_agencies(self):
        """Build the facilities_blockgroups crosswalk file to assign facilities to blockgroups. """
        from ambry.geo.util import find_geo_containment, find_containment

        lr = self.init_log_rate(3000)

        def gen_bound():
            
            boundaries = self.library.dep('blockgroups').partition

            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,boundary in enumerate(boundaries.query(
                "SELECT  AsText(geometry) AS wkt, gvid FROM blockgroups")):
                lr('Load rtree')
     
                yield i, boundary['wkt'] , boundary['gvid'] 
        
        def gen_points():

            for o in self.generate_agencies():
                if o['geocoded']:
                    yield (o['geocoded']['longitude'], o['geocoded']['latitude']), o
          
        p = self.partitions.find_or_new(table='locations')
        p.clean()

        with p.inserter() as ins:
        
            for point, point_o, cntr_geo, cntr_o in find_containment(gen_bound(),gen_points()):
           
                row = point_o['row']
                
                row['gvid'] = cntr_o
           
                ins.insert(row)
        
                lr('Marking point containment')
          