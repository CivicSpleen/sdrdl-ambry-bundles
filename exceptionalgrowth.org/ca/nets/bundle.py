'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def gen_rows(self):
        import csv 
        
        fn = self.filesystem.download('migrations')
        
        with open(fn) as f:
            r = csv.DictReader(f)
            
            for row in r:
                yield row
            

    def meta(self):
        import csv
        
        self.database.create()
    
        self.schema.update('migrations', self.gen_rows(), n=1000, logger = self.init_log_rate(500))
        
        return True
        

    def build(self):
        import uuid
        import random

        p = self.partitions.new_partition(table='migrations')

        p.clean()

        lr = self.init_log_rate(N=5000)

        with p.inserter() as ins:
            for i, row in enumerate(self.gen_rows()):
                
                errors =  ins.insert({k.lower().replace(' ','_'):v for k,v in row.items() })
             
                lr()
            
            
        return True


