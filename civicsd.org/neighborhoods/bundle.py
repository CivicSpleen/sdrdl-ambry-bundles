from  ambry.bundle.loader import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
        
    def build(self):
        
        r = super(Bundle,self).build()
        
        if r:
            self.build_block_cross()
            
        return r
        
            
    def build_block_cross(self):
        """Build the bus_block_cross crosswalk file to assign businessed to blocks. """
        from ambry.geo.util import find_geo_containment

        def generate_geometries():
            """The Containing geometries are the neighborhoods. """

            for row in self.partitions.find(table = 'neighborhoods').query(
                    "SELECT AsText(geometry) as wkt, objectid_1 as id FROM neighborhoods"):

                yield row['id'], row['id'],  row['wkt']

        def generate_points():
            """The points we are going to find the containment of are the centroids of the census blocks"""
            blocks = self.library.dep('blocks').partition
            lr = self.init_log_rate(3000)
            
            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,row in enumerate(blocks.query(
                "SELECT  X(Centroid(geometry)) AS lon,  Y(Centroid(geometry)) AS lat, geoid FROM  blocks")):
                lr('Load rtree')
                
                if self.run_args.test and i > 200:
                    break
                
                yield  (row['lon'], row['lat']), row['geoid'] 

        def mark_contains():
            
            p = self.partitions.find_or_new(table='nhood_block_cross')
            p.clean()
            
            lr = self.init_log_rate(3000)
            
            with p.inserter() as ins:
                while True:
                    (p,point_obj,geometry, poly_obj) = yield # Get a value back from find_geo_containment

                    ins.insert(dict(neighborhoods_id = poly_obj, geoid = point_obj ))
                    lr('Marking point containment')
        
            
        find_geo_containment(generate_geometries(), generate_points(), mark_contains())
    
    

