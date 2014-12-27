'''
'''

from  ambry.bundle import BuildBundle

class Bundle(BuildBundle):
    ''' '''

    def build(self):
        
        self.build_places()

        self.build_geo_cross()

        return True

    def build_places(self):
        import ogr
        from ambry.util import make_acro
        
        import json
        
        col_map = self.filesystem.read_csv(self.filesystem.path('meta','colmap.csv'), ('table','city','outcol'))

        placep = self.partitions.find_or_new_geo(table='places')
        placep.clean()

        parts = [
                 (None,'city', self.library.dep('cities').partition),
                 ('SndSAN','community', self.library.dep('communities').partition),
                 ('SndSAN','council', self.library.dep('councils').partition),
                 ('SndSAN','neighborhood', self.library.dep('neighborhoods').partition)
                 ]

        
        lr = self.init_log_rate(print_rate=5)
        city_union = None
        row_num = 0

        
        with placep.database.inserter('places') as ins:
            for city, type_,  p in parts:
                self.log("Building: {}, {}".format(city, type_))
                past_acronyms = set() 
                table = str(p.identity.table)
                
                mapping = col_map.get( (type_,city,"name"), False )
     
                group_name = mapping['group']
                
                q = """
                SELECT *,
                AsText(Transform(geo, 4326)) AS geometry,
                X(Transform(Centroid(geo), 4326)) AS lon, 
                Y(Transform(Centroid(geo), 4326)) as lat,
                Area(geo) as geo_area
                FROM ( 
                      SELECT *, CastToMultiPolygon(GUnion(Buffer(geometry,0.0))) AS geo 
                      FROM {} 
                      GROUP BY {} 
                      ORDER BY Area(geo) DESC
                )
                """.format(table, group_name)
      
                for i, row in enumerate(p.database.query(q)):
                    drow = dict(row)

                    for oc in ('scode', 'code','name'):
 
                        mapping = col_map.get( (type_,city,str(oc)), False )
    
                        ic = mapping['incol']
                        
                        if '{' in ic:
                            v = ic.format(i=i,acro='***',**drow)
                        else:
                            v = drow[ic]
                        
                        drow[oc] = v

            
                    drow['name'] = drow['name'].title()
                
                    if '***' in drow['code']:
                        acro = make_acro(past_acronyms,city,drow['name'])
                        drow['code'] = drow['code'].replace('***',acro.upper())
                    
                    drow['area'] = drow['geo_area']
                    drow['type'] = type_
                    
                    if type_ == 'city':
                        drow['city'] = drow['code'] 
                    else:
                        drow['city'] = city
                    
                    
                        
                    row_num += 1
                    drow['id'] = row_num
                    
                    ins.insert(drow)
                    lr(city)

                    if type_ == 'city' and drow['code'] != 'SndSDO':
                       
                        g = ogr.CreateGeometryFromWkt(drow['geometry'])
                        
                        g = g.SimplifyPreserveTopology(.00001)
                      
                        if not city_union:
                            city_union  = g
                        else:
                            city_union = city_union.Union(g)
                            
                            

        self.log("Done building")


    def build_geo_cross(self):
        from ambry.geo.util import find_geo_containment, find_containment

        

        def gen_bound():
            
            boundaries = self.partitions.find(table='places')

            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,boundary in enumerate(boundaries.query("SELECT  id, AsText(geometry) AS wkt FROM places")):
                lr('Load rtree')
     
                yield i, boundary['wkt'] , boundary['id'] 
        
        def gen_points(geography):

            # HACK! This isn't quite right. We should be using the census-created interior point, 
            # which is guaranteed to be inside the shape. Centroids can be outside the shape, although that rarely
            # happens for blockgroups. 
            
            q = """
            SELECT *,
            AsText(Transform(geometry, 4326)) AS geometry,
            X(Transform(Centroid(geometry), 4326)) AS lon, 
            Y(Transform(Centroid(geometry), 4326)) as lat,
            Area(geometry) as area
            FROM  {}""".format(geography)

            for row in self.library.dep(geography).partition.query(q):
                if  row['lon'] and row['lat']:
                    yield (row['lon'], row['lat']), row['gvid']


        lr = self.init_log_rate(200)

        p = self.partitions.find_or_new(table='places_blockgroups')
        p.clean()

        
        with p.inserter() as ins:
            for point, point_o, cntr_geo, cntr_o in find_containment(gen_bound(),gen_points('blockgroups')):

                ins.insert(dict(places_id = cntr_o, gvid = point_o))
            
                lr('Assigning to blockgroups')
        
        
        p = self.partitions.find_or_new(table='places_tracts')
        p.clean()

        
        with p.inserter() as ins:
            for point, point_o, cntr_geo, cntr_o in find_containment(gen_bound(),gen_points('blockgroups')):

                ins.insert(dict(places_id = cntr_o, gvid = point_o))
            
                lr('Assigning to tracts')
        
        
        
        
        
        
        
