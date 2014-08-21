'''

'''

from  ambry.bundle.geo import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
    
    def build(self):
        
        super(Bundle, self).build()
        self.build_acs_crosswalk()
        
        return True

    def build_acs_crosswalk(self):
        
        from ambry.geo.util import find_geo_containment

        def generate_geometries():
            
            blocks = self.partitions.find(table='sws_boundaries')
            lr = self.init_log_rate(3000)
            
            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,block in enumerate(blocks.query("SELECT  AsText(geometry) AS wkt, id FROM  sws_boundaries")):
                lr('Load rtree')
                
                if self.run_args.test and i > 200:
                    break
                
                yield block['id'],block['id'], block['wkt']

        def generate_blockgroups():
            """Generate centroids of the 2012 ACS blockgroups"""
            
            block_groups = self.library.dep('bg2012').partition
         
            for row in block_groups.query("""SELECT 
                    geoid, 
                    X(Transform(Centroid(geometry), 4326)) AS lon, 
                    Y(Transform(Centroid(geometry), 4326)) as lat,
                    MbrMinX(geometry) AS x_min, 
                    MbrMinY(geometry) AS y_min, 
                    MbrMaxX(geometry) AS x_max,  
                    MbrMaxY(geometry) AS y_max
                    FROM blockgroups
                    WHERE arealand > 0
                    """):
                if  row['lon'] and row['lat']:
                   
                    yield (row['x_min'], row['y_min'], row['x_max'], row['y_max']), row['geoid']

        def mark_contains():
            
            p = self.partitions.find_or_new(table='acs_cross')
            
            p.clean()
            
            with p.inserter() as ins:
            
                while True:
                    (p,point_obj,geometry, poly_obj) = yield # Get a value back from find_geo_containment

                    d = {
                        'geoid': point_obj,
                        'sws_boundaries_id': poly_obj
                    }


                    ins.insert(d)
    
        self.log("Linking ACS tracts to boundaries")
        find_geo_containment(generate_geometries(), generate_blockgroups(), mark_contains(), method = 'intersects')
    