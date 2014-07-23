'''
'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def build(self):
        from databundles.identity import PartitionIdentity
        
        self.partitions.new_geo_partition( PartitionIdentity(self.identity, table='pitc10'), 
                                           shape_file = self.config.build.sources.pitc10)

        return True
     
    def make_hdf(self):
        
        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area
        from osgeo.gdalconst import GDT_Float32
        import numpy as np
        place='SndSAN'

        aa = get_analysis_area(self.library, place=place, scale=10)
        trans = aa.get_translator()
        a = aa.new_array()

        k = dg.GaussianKernel(33,11)

        k.matrix *= ( 1000000  / aa.scale**2 ) # Convert to count / km^2

        p = self.partitions.find(table='pitc10')

        lr = self.init_log_rate()
        for row in p.query("""SELECT *, 
         Y(Transform(geometry, 4326)) as lat, X(Transform(geometry, 4326)) as lon
         FROM pitc10"""):
            p =  trans(row['lon'], row['lat'])
            k.apply_add(a,p)  
            lr("Add raster point")

        raster = self.partitions.new_hdf_partition(table='pitc10r') 
               
        raster.database.put_geo(place, a, aa)

    def extract_image(self):
        
        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area

        place = 'SndSAN'
        
        raster = self.partitions.find(table='pitc10r')        
        a, aa = raster.database.get_geo(place)

        file_name = self.filesystem.path('extracts', 'kde.tiff')
        
        aa.write_geotiff(file_name, a[:])
        
        return file_name
    
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    