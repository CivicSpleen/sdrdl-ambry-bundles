'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def gen_objects(self):
        import json
        
        projects = self.library.dep('projects').partition
         
        def augment(path,o, parent=None):
            """Descend recursively"""
            from ambry.identity import ObjectNumber
            
            path_num = ObjectNumber.base62_decode(''.join( ObjectNumber.base62_encode(e[1]).zfill(2) for e in path ))
                 
            objects = []

            if isinstance(o, dict):

                for i,(k,v) in enumerate(o.items(),1):
                    objects += augment(path+[ (k,i,) ], v, k)
                    
                o['path'] = tuple(path)
                
                o['path_num'] = path_num
                o['parent'] = parent if parent else 'root'
                objects += [{ k:v for k,v in o.items()   if not isinstance(v,(list,dict))}]
                                 
            elif isinstance(o, list):
                for i, v in enumerate(o,1):
                    objects += augment(path+[ (i,i,) ], v, parent)
                    
            return objects

        for i,row in enumerate(projects.rows):
            
            d = json.loads(str(row.data).decode('zlib').decode('utf8'))
       
            objects = augment([],d)
            
            project_id = d['ProjectId']
            
            for o in objects:
                
                if 'ProjectId' in o:
                    assert o['ProjectId'] == project_id
                else:
                    o['ProjectId'] = project_id
                
                yield  o
            
            
    def dump(self):
        
        for o in self.gen_objects():
            if o['parent'] == 'Invoices':
                print o['ProjectId'], o['parent'], o
                

    
