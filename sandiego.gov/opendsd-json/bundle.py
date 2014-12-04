'''
Extract JSON from the OpenDSD 
'''

from  ambry.bundle import BuildBundle
from Queue import Queue, Full, Empty
import threading
import signal
from multiprocessing.pool import ThreadPool


def requestor_thread(idn, url, json_queue):
    import requests

    r = requests.get(url, headers={'Accept':'application/json'})

    json_queue.put((idn, url, r.text))   


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
        

    def url_generator(self):
        """Read project ids from the upstream dataset, then put URLs to get the project data
        onto a queue, at a maximum rate. The request threads will take URLs off of that queue.  """
        
        import time
        
        tok_n = 0
        
        apps = self.library.dep('applications').partition

        last_time = time.time()
        request_count = self.requests_per_delay
        
        def yield_urls():
        
            for i, row in enumerate(apps.query('SELECT DISTINCT project_id FROM applications '
                                           'ORDER BY project_id ')):
            
                yield i, self.metadata.sources.get('project').url.format(id=int(row['project_id']))

        g = yield_urls()

        while True:
            # wait for the next time slot
            dt =  last_time + self.delay_time - time.time()
         
            if dt > 0:
                time.sleep(dt)
    
            last_time = time.time()

            # Generate all of the URLs for this slot. 
            for ri in range(self.requests_per_delay):
    
                yield g.next()        
         
           
    def test_url_generator(self):
        import time 
        
        lr = self.init_log_rate(50)
        
        for i, url in self.url_generator():
           
            lr(str(url))
       
    
    def generate_json(self):
      
        import time 
        
        lr = self.init_log_rate(50)
        
        for idn, url in self.url_generator():
          
            # Limit the number of active threads
            while threading.active_count() > 15:
                time.sleep(.1)
      
            t  = threading.Thread(target=requestor_thread, args=(idn, url, self.json_queue))
            t.start()    

            while self.json_queue.qsize() > 0:
                try:
                    yield self.json_queue.get()
                except Empty:
                    break
                
            
    def build(self):
        import time 
        
        lr = self.init_log_rate(50)

        p = self.partitions.find_or_new(table='json', grain='projects')

        with p.inserter(cache_size = 10) as ins:
            for idn, url, json in self.generate_json():
    
                lr(url)
        
             
                d = dict(
                    id = idn,
                    type = 'project',
                    object_id = idn,
                    access_time = time.time(),
                    data = json.encode('utf8').encode('zlib')
                )
    
            
                ins.insert(d)
              