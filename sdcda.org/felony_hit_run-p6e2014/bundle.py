'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta(self):
        self.meta_gen_schema()
        return True

    def meta_gen_schema(self):
        
        self.database.create()
    
        self.schema.update('hitnrun', self.gen_rows(), logger = self.init_log_rate(500))

    def meta_calc_jail(self):
        """Create a blank map for the sentencing values. """
        import re
        orig = self.partitions.find(table='hitnrun', grain='orig')
        
        types = set()
        
        snmap = self.filesystem.read_yaml('meta/sentence_map.yaml')
        
        for row in orig.rows:
            sntc = row['sentencing']
            if sntc:
                parts = [x.strip() for x in sntc.split('-')]
                for part in parts:
                    if not ':' in part:
                        continue
                 
                    type_, time = [str(x) for x in part.split(':',1)]
                    types.add(type_)
                    
                    if ':' in time:
                        times = [x.strip() for x in time.split(':')]
                    else:
                        times = [time.strip()]
                        
                    days = 0
                    for time in times:
                        
                        match = re.match(r'(\d+)\s+(\w+)', time)
                        
                        if not match:
                            print "FAILED: ", time
                            
                        n = int(match.group(1))
                        p = match.group(2)
                        
                        if p == 'Days':
                            days += n
                        elif p == 'Years':
                            days += n*365
                        elif p == 'Months':
                            days += n*30.4
                        
                    #print snmap[type_], days
                        
                        
                    
                    
        #import yaml
        #print yaml.dump({k:'' for k in types},default_flow_style=False, indent=4, encoding='utf-8')

        for v in snmap.values():
            parts = v.split("_")
            while parts:
                print '_'.join(parts)
                parts.pop()
            

    def gen_rows(self,):
        '''If accumulate is true, each new row is overlaid on the previous row, to fill in 
        all the elements in the heirarchy. '''
        from xlrd import open_workbook
        
        f = self.filesystem.download('fhnr')
        
        wb = open_workbook(f)
        
        sheet = None
        
        sheet = [s for s in  wb.sheets()][1]
        
        header = None
        for i,row in enumerate(range(sheet.nrows)):
        
            
            values = []
            for col in range(sheet.ncols):
                values.append(sheet.cell(row,col).value)
 
            if i == 0:
                header = [ i.replace(' ','_').lower() for i in values]
                continue
            
            yield dict(zip(header, values) )
            

    def build(self):
        import datetime 
        import xlrd
        p = self.partitions.find_or_new(table='hitnrun', grain='orig')
        p.clean()
        
        with p.inserter() as ins:
            for r in self.gen_rows():
                
                dt =  xlrd.xldate_as_tuple(int(r['offense_date']), 0)
                r['offense_date']  = datetime.datetime(*dt)
                
                ins.insert(r)
                
        self.build_cleaned()
                
        return True
        
    def build_proc_sentence(self, snmap, sntc):
        """Break the sentencing field in to parts, maps to columns names, and turn the time values into days. """
        import re
        
        # '-' seperates parts of the sentence. 
        parts = [x.strip() for x in sntc.split('-')]
        new_row = {}
        for part in parts:
            
            if not ':' in part: # All useful parts have a ':' seperating the type form the time. 
                continue
 
            # Break up the Sentenceing field, and convert the values
            # to times in our heirarchical fields. 
            
            type_, time = [str(x) for x in part.split(':',1)] # Break on the first colon. 

            # Sometimes, the time field has two period values: "5 Years: 3 Days"
            if ':' in time:
                times = [x.strip() for x in time.split(':')]
            else:
                times = [time.strip()]
        
            days = 0
        
            # COnvert everything to days. 
            for time in times:
        
                match = re.match(r'(\d+)\s+(\w+)', time)
        
                if not match:
                    print "FAILED: ", time
            
                n = int(match.group(1))
                p = match.group(2)
        
                if p == 'Days':
                    days += n
                elif p == 'Years':
                    days += n*365
                elif p == 'Months':
                    days += n*30.4
        
            # The column names are heirarchical. Assign the days value to each level of the heirarchy. 
            
            parts = snmap[type_].split("_")
            while parts:
                k = '_'.join(parts)

                if k in new_row and new_row[k] is not None:
                    new_row[k] += days
                else:
                    new_row[k] = days
                    
                parts.pop()
                
        return new_row

    def build_cleaned(self):
        """Create a blank map for the sentencing values. """
        
        orig = self.partitions.find(table='hitnrun', grain='orig')
    
        cleaned = self.partitions.find_or_new(table='hitnrun', grain='cleaned')
        cleaned.clean()

        snmap = self.filesystem.read_yaml('meta/sentence_map.yaml')
        lr = self.init_log_rate(100, 'cleaning')
        with cleaned.inserter() as ins:
            for row in orig.rows:
                row = dict(row)
                if row['sentencing']:
                    upd = self.build_proc_sentence(snmap, row['sentencing'])
                    row.update(upd)
                            
                if ',' in row['highest_charge']:
                    row['highest_charge'], row['highest_charge_mod'] = [i.strip() for i in row['highest_charge'].split(',')]
                            
                ins.insert(row)
                lr()
 
        


                    
                    
                    
            
        
    
        
            
        
            
