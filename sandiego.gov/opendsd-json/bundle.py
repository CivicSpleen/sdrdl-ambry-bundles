'''
Extract JSON from the OpenDSD 
'''

from  ambry.bundle import BuildBundle
from Queue import Queue, Full, Empty
import threading
import signal
from multiprocessing.pool import ThreadPool

class Bundle(BuildBundle):
    ''' '''
    
    segment_size = 20000

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)
        
        self.last_time = None
        # API docs say the rate limit is 20 requests per 200 ms, or 100/s, but the server 
        # doesn't seem to respond at more than 20/s
        self.delay_time = .2
        self.requests_per_delay = 10
      
        # Queue and thread for writing
        self.json_queue = Queue(1000)
        

    def generate_project_urls(self, p):
        """Generate URLs for project objects we haven't seen yet. """
    
        apps = self.library.dep('applications').partition
    
        extant = set([ row['object_id'] for row in p.query("SELECT object_id FROM json ") ])

        limit = "LIMIT 200" if self.run_args.test else ''

        for i, row in enumerate(apps.query('SELECT DISTINCT project_id FROM applications '
                                       'ORDER BY project_id ' + limit)):
        
            if int(row['project_id']) in extant:
                continue
        
            yield i, int(row['project_id']), self.metadata.sources.get('project').url.format(id=int(row['project_id']))

    def generate_approval_urls(self, p):
        """Generate URLs for approval objects we haven't seen yet. """
    
        apps = self.library.dep('completed').partition
    
        extant = set([ row['object_id'] for row in p.query("SELECT object_id FROM json") ])

        limit = "LIMIT 200" if self.run_args.test else ''

        for i, row in enumerate(apps.query('SELECT DISTINCT approval_id FROM permits '
                                       'ORDER BY approval_id '+limit )):
        
            if int(row['approval_id']) in extant:
                continue
        

            yield i, int(row['approval_id']), self.metadata.sources.get('approval').url.format(id=int(row['approval_id']))
            
    def generate_invoice_urls(self, p):
        import json 
        
        p1 = self.partitions.find_or_new(table='json', grain='approvals')
        p2 = self.partitions.find_or_new(table='json', grain='projects')
        
        extant = set([ row['object_id'] for row in p.query("SELECT object_id FROM json") ])
        
        invoice_ids = set()
        
        for p in (p1, p2):
            for row in p.rows:
        
                d = json.loads(str(row['data']).decode('zlib').decode('utf-8'))
            
                #print str(row['data']).decode('zlib').decode('utf-8')
            
                af =  d.get('ApprovalFees',None)
                
                if not af:
                    af =  d.get('Invoices',None)
            
                if af:
                    for f in af:
                        invoice_ids.add(f.get('InvoiceId'))

        
        for i in  invoice_ids - extant:
            yield i, int(i), self.metadata.sources.get('invoice').url.format(id=i)
            
    def rate_limit_generator(self, g):
        """Read URLs from the upstream generator, then put URLs to get the project data
        onto a queue, at a maximum rate. The request threads will take URLs off of that queue.  """
        
        import time

        last_time = time.time()
        request_count = self.requests_per_delay
     
        while True:
            # wait for the next time slot
            dt =  last_time + self.delay_time - time.time()
         
            if dt > 0:
                time.sleep(dt)
    
            last_time = time.time()

            # Generate all of the URLs for this slot. 
            for ri in range(self.requests_per_delay):
    
                yield g.next()        

    def test_rate_limit(self):
        
        def generate():
            for i in range(1000):
                yield i
            
        lr = self.init_log_rate(50)
            
        for x in self.rate_limit_generator(generate()):
            lr(str(x))
            self.progress(str(x))


    def generate_json(self, g):
      
        import time 
        
        lr = self.init_log_rate(50)
        
        for idn, object_id, url in self.rate_limit_generator(g):
          
            # Limit the number of active threads
            while threading.active_count() >= 5:
                self.progress("Sleeping. {} threads. Last idn: {}".format(threading.active_count(), idn ))
                time.sleep(.1)
                
                
            def requestor_thread(idn, object_id, url, json_queue):
    
                import requests


                try:
                    r = requests.get(url, headers={'Accept':'application/json'})

                    r.raise_for_status()

                    json_queue.put((idn, object_id, url, r.status_code, r.text))
                    
                except Exception as e:
                    import json
                    #self.error("Failed to request: {}: {}".format(url, e))
                 
                    json_queue.put((idn, object_id, url, r.status_code, json.dumps(dict(
                        error = r.status_code,
                        message = e.message,
                        body = r.text
                    ))))
                    
      
            t  = threading.Thread(target=requestor_thread, args=(idn, object_id, url, self.json_queue))
            t.start()    

            while self.json_queue.qsize() > 0:
                self.progress('Getting from queue {}'.format(self.json_queue.qsize()))
                try:
                    yield self.json_queue.get()
                except Empty:
                    break
                
    def scrape_api(self,object_type,p,g):
        """Run the json generator and store the json records in the database. """
        import time 
        lr = self.init_log_rate(50)
        
        with p.inserter(cache_size = 10) as ins:
            for idn, object_id, url, response_code, json in self.generate_json(g):
    
                self.progress('Inserting {} {}'.format(response_code,url))
    
                lr("{} {} ".format(idn, url))

                d = dict(
                    id = object_id,
                    type = object_type,
                    object_id = object_id,
                    access_time = time.time(),
                    response_code = response_code,
                    data = json.encode('utf8').encode('zlib')
                )
    
                ins.insert(d)
            

    
    def build(self):

        p = self.partitions.find_or_new(table='json', grain='approvals')
        g = self.generate_approval_urls(p)
        
        self.scrape_api('approval',p,g)

        p = self.partitions.find_or_new(table='json', grain='projects')
        g = self.generate_project_urls(p)
        
        self.scrape_api('project',p,g)
        
        p = self.partitions.find_or_new(table='json', grain='invoices')
        g = self.generate_invoice_urls(p)
        
        self.scrape_api('invoice',p,g)
        
        return True
     