'''

'''

from  ambry.bundle import BuildBundle
import csv
import datetime

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        self.part_cache = {}


    def generate_incidents(self, p):
        from ambry.client.ckan import Ckan
      
        repo = Ckan(self.metadata.build.repo.url, self.metadata.build.repo.key)
   
        pkg = repo.get_package(self.metadata.build.repo.package)
        
        for resource in pkg['resources']:    
                  
            f = self.filesystem.download(resource['url'])
            uz = self.filesystem.unzip(f)
                   
            self.log("Reading: {}".format(uz))

            with open(uz, 'rbU') as csvfile:
                reader = csv.reader(csvfile)
                header = reader.next()
                
                fh = [ c.data['fileheader'] for c in p.table.columns]
                # get rid of the id field, since that isn't in the data. 
                fh = fh[1:]
                if  fh != header:
                    raise Exception("Header mismatch: {} != {} ".format(fh, header))
                
                for row in reader:
                    yield  list(row)

    def get_partition(self,year):
        
        if year not in self.part_cache:
            p = self.partitions.find_or_new(time=year, table='incidents');
            self.part_cache[year] = p.database.inserter('incidents')
            
        return self.part_cache[year]

    def build(self):
        from dateutil.parser import parse
        
        
        lr = self.init_log_rate(10000)
        
        # All incidents
        allp = self.partitions.find_or_new(table='incidents');
        allins = allp.database.inserter()
        
        table = allp.table

        header = [c.name for c in table.columns]
        
        for row in self.generate_incidents(allp):
            
            
            lr()

            dt = parse(row[2])
            row[2] = dt
            row[5] = unicode(row[5],errors='ignore').strip()
            
            ins = self.get_partition(dt.year)

            drow = [ v if v else None for v in row ]

            if not drow[6]:
                drow[6] = -1 # Zips

            # The [None] bit is the place holder for the id column
            drow = dict(zip(header, [None]+drow))

            try:
                ins.insert(drow)
                allins.insert(drow)
            except:
                print row
                raise

        for ins in self.part_cache.values():
            ins.close()

        allins.close()

        return True

import sys

if __name__ == '__main__':
    import ambry.run
      
    ambry.run.run(sys.argv[1:], Bundle)
     
    
    