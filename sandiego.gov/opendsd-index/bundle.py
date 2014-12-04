'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)


    def state_pa(self):

        st = set()

        for key in self.metadata.sources:
            
            if key == 'base':
                continue
            
            year, state, pa = key.split('_')
            
            st.add((state,pa))
            
        return st
          
    def years(self):
        
        years = set()

        for key in self.metadata.sources:
            
            if key == 'base':
                continue
            
            year, state, type_ = key.split('_')
            
            years.add(int(year))
            
        return years

    def meta_sources(self):
        """Read the data stored in S3 to generate a list of sources. """
        from lxml import etree
        from lxml import objectify
        from ckcache import new_cache
        
        c = new_cache(self.metadata.sources.base.url)
        
        for e in c.list():
            try:
                year, fn = e.split('/')
            except ValueError: # Got s3 directory, but no files
                continue
            
            
            _,year2,dsd,type_,state = fn.strip('.xml').split('_')
            
            key = "{}_{}_{}".format(year, state.lower(), type_.lower())
            
            self.metadata.sources[key] = dict(
                url = c.path(e, public_url=True),
                description = "{} {} {}".format(year, state, type_)
            )
            
            self.update_configuration()

    def gen_rows(self, year, state, pa, as_dict = True ):
        
        from lxml import etree
        from lxml import objectify
        import xmltodict
        from dateutil.parser import parse as parse_date
        
        headers = {}
        for key in self.metadata.sources:
            
            if key == 'base':
                continue
            
            y, s, p = key.split('_')
        
            if year != int(y) or s != state or p != pa:
                continue
        
            fn = self.source(key)
        
            with open(fn) as f:
       
                root = xmltodict.parse(f.read())
            
                for i, v in enumerate(root['extract_results']['approvals']['approval']):
                    v['state'] = state
                    v['approval_id']  = v['@approval_id']
                    del  v['@approval_id']
                
                    v['state'] = state
                    v['pa'] = pa.lower()
                    v['year'] = int(year)
                    
                    for k in v.keys():
                        if k.endswith('_date'):
                            v[k] = parse_date(v[k])
                            
                    if as_dict:
                        yield v
                    else:
                        yield v.values()

    def meta_build_schema(self):
        
        self.prepare()
        
        for state, pa in self.state_pa():
            self.log("Updating for {} {}".format(state, pa))
            
            self.schema.update(pa, self.gen_rows(2004, state, pa), n = 1000, 
                                logger = self.init_log_rate(500))
            
            
    def build(self):
        
        for state, pa in self.state_pa():
            
            p = self.partitions.find_or_new(table=pa, grain=state)
            
            for year in self.years():

                self.log("Starting {} {} {}".format(year, state, pa))

                lr = self.init_log_rate(10000)
                
                with p.inserter() as ins:
                    
                    for row in self.gen_rows(year, state, pa):
                        
                        try:
                            errors = ins.insert(row)
                        except:
                            self.error("Failed for {} ".format(p.identity))
                            
                        if errors:
                            self.error("insert Errors: ", error)
                
        
                        lr(str(p.identity))
                        
                        
        return True
        
        
