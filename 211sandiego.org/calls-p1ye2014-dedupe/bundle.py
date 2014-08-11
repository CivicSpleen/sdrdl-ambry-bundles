'''
Group 211 calls by client id and time. 
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    dupe_cols = set([u'ref_agency',  u'ref_name', 
                     u'need_unmet', u'need_reason_unmet',
                     u'need_tax_code', u'need_tax_cat',
                     u'ins_type',
                     u'income_source'])

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        from collections import defaultdict

        p = self.library.dep('calls').partition
        
        recs = defaultdict(list)
        
        lr = self.init_log_rate(N=100000)
        
        for i, row in enumerate(p.query('SELECT * from calls')):
            recs[(row['create_time'], row['client_id'])].append(row)
            
            lr()
            
            if i > 100000:
                break

        
        dupe_cols = set()
        
        for (t, client_id), rows in recs.items():
            
            if len(rows) == 1:
                continue
            
            r = defaultdict(set)
            
            for row in rows:
                for name, v in row.items():
                    r[name].add(v)
                    
            
            dupe_cols.update([ k  for k,v in r.items() if len(v) != 1 ])
                    
        print dupe_cols


       
        return True

