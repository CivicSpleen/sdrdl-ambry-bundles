'''
Group 211 calls by client id and time. 
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    dupe_cols = {
        'agencies' : [ 'ref_agency',  'ref_name' ],
        'unmet_need' : ['need_unmet', 'need_reason_unmet'],
        'need_tax' :  ['need_tax_code', 'need_tax_cat' ],
        'income_source' : ['income_source'],
        'ins_type' : ['ins_type']
    }

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        
        from collections import defaultdict

        p = self.library.dep('calls').partition
        
        recs = {}
        
        lr = self.init_log_rate(N=100000)
        
        for i, row in enumerate(p.query('SELECT * from calls')):
            
            k = (row['create_time'], row['client_id'])
          
            if not k in recs:
                
                recs[k] = (row, {
                    'agencies' : set(),
                    'unmet_need' : set(),
                    'ins_type' : set(),
                    'need_tax' : set(),
                    'income_source' : set()
                })
                
            recs[k][1]['agencies'].add( (row['ref_agency'], row['ref_name']) )
            recs[k][1]['unmet_need'].add((row['need_unmet'], row['need_reason_unmet']))
            recs[k][1]['need_tax'].add((row['need_tax_code'], row['need_tax_cat']))
            recs[k][1]['ins_type'].add((row['ins_type'],))
            recs[k][1]['income_source'].add((row['income_source'],))
            

            lr('Loading and deduping')
            
            if self.run_args.test and i > 50000:
                break
               
        lr = self.init_log_rate(N=10000)
                
        p = self.partitions.find_or_new(table = 'calls', tables = self.dupe_cols.keys())
        p.clean()
        self.log("Inserting deduped calls")
        
        with p.inserter() as ins:
            for i, (k, (row, multi_vals)) in enumerate(recs.items()):
                if self.run_args.test and i > 50000:
                    break
                
                ins.insert(row)
                lr('calls')
                
        for table_name, cols in self.dupe_cols.items():
       
            table = self.schema.table(table_name)
       
            cols = [c.name for c in table.columns]
            
            self.log("Inserting into {}".format(table_name))

            with p.inserter(table_name) as ins:
                for i, (k, (row, multi_vals)) in enumerate(recs.items()):
                 
                    if self.run_args.test and i > 50000:
                        break  
                    for vals in multi_vals[table_name]:
                        if any(vals):
                            mv_row = [None, row['id']] + list(vals)

                            ins.insert( dict(zip(cols, mv_row)))
                    
                            lr(table_name)

       
        return True

