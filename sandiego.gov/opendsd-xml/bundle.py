'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
    
    segment_size = 20000

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)
        
        self.last_time = None
        self.delay_time = 0.01
        
    def build(self):
        
        self.request_projects()
        
        return True
        
    def request_projects(self):
        import time
        
        apps = self.library.dep('applications').partition
        
        p =  None
        
        lr = self.init_log_rate(10)

        for i, row in enumerate(apps.query('SELECT DISTINCT project_id FROM applications ORDER BY project_id')):
        
            id = int(row['project_id'])
        
            this_segment = int( i / self.segment_size ) + 1
        
            if not p or this_segment != p.identity.segment:
                if p and not p.is_finalized:
                    p.finalize()
                    p.close()
                    ins.close()
                    
                p = self.partitions.find_or_new(table='xml', grain='projects', segment = this_segment)
                ins = p.inserter()
                ins.__enter__()
                self.log("Switching to {}".format(str(p.identity)))
                
            if p.is_finalized:
                # skip it, and iterate through until we get to a record for a segment that is not finalized
                continue
            else:
                lr(str(p.identity))
        
            t1 = time.time()
            
            data = self.request('project', row['project_id'])
            
            diff = time.time() - t1
            
            self.log('{} bytes, {} s'.format(len(data),diff))
        
            ins.insert(dict(
                id = id,
                type = 'project',
                object_id = id,
                data = data
            ))
            
        p.close()
        ins.close()
        
    def request(self, url_name, id):
        import requests
        import time
     
        if not self.last_time:
            self.last_time = time.time()
        
        t_d = self.last_time + self.delay_time - time.time() 
        
        if t_d > 0:
            time.sleep(t_d)
        
        url = self.metadata.sources.get(url_name).url.format(id=id)
        
        self.last_time = time.time()
        
        r = requests.get(url)
        return r.text
        