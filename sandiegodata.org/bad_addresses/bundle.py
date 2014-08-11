'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        
        p = self.partitions.find_or_new(table='addresses')
        p.clean()
        
        self.build_masterlist(p)
        #self.build_alcohol(p)
        
        return True

    def build_masterlist(self, p):
        from address_parser import Parser
        from ambry.geo.geocoder import Geocoder
        
        gp = self.library.dep('geocoder').partition
        
        g = Geocoder(gp)
        
        ap = Parser()
        
        
        ip = self.library.dep('masterlist').partition
        lr = self.init_log_rate(1000)
     
     
        streets = set()
        
        with p.inserter() as ins:
            for row in ip.query("SELECT * FROM businesses WHERE address_id IS NULL"):
            
                row = dict(row)
            
                row['city'] = row['city'].strip().title() if row['city'] else ''
            
                if row['city'].strip().title() == 'La Jolla':
                    row['city'] = 'San Diego'

            
                ps = ap.parse(row['address'], row['city'], row['state'], row['zip'])
                
                try:
                    address_id, result, parsed = g.parse_and_code(str(ps))
                     
                except AttributeError as e:
                    print e
                    raise
                    continue
                    
                
                d = ps.args

                d['text'] = str(ps)
                d['orig_text'] = "{}, {}, {} {}".format(row['address'], row['city'], row['state'], row['zip'])
                d['source'] = 'sdbml'
                d['address_id'] = address_id

                k = (d['direction'], d['name'], d['suffix'])

                if not k in streets:
                    streets.add(k)
                    
                    d['for_testing'] = 'y'

                
                ins.insert(d)
                lr()
                
                #print ps
             
                
        return True


    def build_alcohol(self, p):
        from address_parser import Parser
        from ambry.geo.geocoder import Geocoder
        
        gp = self.library.dep('geocoder').partition
        
        g = Geocoder(gp)
        
        ap = Parser()
        
        
        ip = self.library.dep('alcohol').partition
        lr = self.init_log_rate(1000)
        
        with p.inserter() as ins:
            for row in ip.query("SELECT * FROM licenses"):
            
                lr()
                
                if not row['premisesaddress']:
                    continue
                

                try:
                    address_id, result, parsed = g.parse_and_code(row['premisesaddress'])
                except AttributeError as e:
                    print e
                    continue
                    
               
                
                d = parsed.args
                
                d['text'] = str(parsed)
                d['orig_text'] = row['premisesaddress']
                d['source'] = 'alco'
                d['address_id'] = address_id
                if result:
                    d['score'] = result['score']
                
                ins.insert(d)

                
        return True



    def test_geocode(self):
        
        import dstk
        
        p = self.partitions.find(table='addresses')
    
        dstk = dstk.DSTK({'apiBase':'http://ec2-54-235-229-124.compute-1.amazonaws.com/'})
        
        c = []
        
        lr = self.init_log_rate(300)
        
        o = {}
        
        for row in p.rows:
           
            c.append(str(row.text))

            if len(c) > 10:
           
                try:
                    r = dstk.street2coordinates(c)
                    o.update(r)
                except UnicodeDecodeError:
                    # The unicode errors occur for strings that don't appear to have unicode,
                    # so I don't know how to fix it. 
                    for x in c:
                        try:
                            dstk.street2coordinates(x)
                            o.update(r)
                        except UnicodeDecodeError:
                            self.error("Failed for '{}'".format(x))  
                        
                c = []
            
            lr()
                
                
        self.log("Done. {} records".format(len(o)))
        
        
