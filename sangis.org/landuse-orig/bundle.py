'''
'''

from  ambry.bundle.geo import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
    pass


    def copy_currentuse(self):
        import os 
        
        cuo = self.partitions.find(table='currentuse_orig')
        
        cu = self.partitions.find_or_new_geo(table='currentuse')


        srs = cuo.get_srs()

        lr = self.init_log_rate(N=2000, message="Copy orig")

        try: cu.query("DELETE FROM  currentuse")
        except: pass
        
        # Using ExteriorRing because there is an error in an internal ring in one of the parcels. 
        with cu.database.inserter(source_srs=srs, dest_srs=srs) as ins:
            for row in cuo.query("""
            SELECT lu as code, landuse as name, AsText(geometry) AS wkt, Area(geometry) AS area
            FROM currentuse_orig
            """):

                lr()
           
                ins.insert(dict(row))
  