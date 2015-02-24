'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 

class Bundle(ExcelBuildBundle):
    ''' '''

    decode = lambda self, v : v.encode('ascii', 'ignore').decode('ascii')

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def foo(self):
        import csv
        from ambry.geo.geocoders import DstkGeocoder
        from collections import defaultdict
        
        fn  = self.source('agency_list')
        
        names = {}
        site_id = defaultdict(int)
        
        def address_gen():
            with open(fn) as f:
                r = csv.DictReader(f)

                for row in r:
                    row['id'] = row['id'].strip()
                    row['name'] = row['name'].strip()
                    
                    if row['id']:
                        if row['name'] in names:
                            self.error("Dupe name:".format(row['name']))
                    
                        else:
                            names[row['name']] = row
                            site_id[row['name']] += 1
                            row['site_id'] = site_id[row['name']]
                                                    
                    else:
                        row['id'] = names[row['name']]['id']
                        site_id[row['name']] += 1
                        row['site_id'] = site_id[row['name']]
                    
                    yield (row['address'].decode('ascii','ignore'), (row['id'], row['site_id'], row['name']))
                
        dstk_service = self.config.service('dstk')
        
        dstk_gc = DstkGeocoder(dstk_service, address_gen())

        with open("agencies.csv", 'w') as f:
            w = csv.writer(f)
            w.writerow('agency_id site_id name orig_address geocoded_address'.split())
            for i, (k, r, o) in enumerate(dstk_gc.geocode()):
                
                row = [o[0],o[1],o[2],k]
                
                if r:
                    row += [r['street_address'], r['locality'], r['latitude'], r['longitude']]
                
                
                w.writerow(row)
               
          
          