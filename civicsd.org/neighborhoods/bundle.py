from  ambry.bundle.geo import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
    
                 
    def build_block_cross(self):
        """Build the bus_block_cross crosswalk file to assign businessed to blocks. """
        from ambry.geo.util import find_geo_containment

        def generate_geometries():
            
            blocks = self.library.dep('blocks').partition
            lr = self.init_log_rate(3000)
            
            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,block in enumerate(blocks.query("SELECT  AsText(geometry) AS wkt, geoid FROM  blocks")):
                lr('Load rtree')
                
                if self.run_args.test and i > 200:
                    break
                
                yield i, block['geoid'] , block['wkt']
        
        def generate_points():
            p = self.partitions.find(table = 'dstk_addresses')
            #p = self.library.get('sandiego.gov-businesses-orig-dstk_addresses-1.0.3').partition
            for row in p.rows:
                if  row['lon'] and row['lat']:
                    yield (row['lon'], row['lat']), row['businesses_id']

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
    
    

