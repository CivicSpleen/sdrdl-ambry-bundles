'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def gen_rows(self):
        from xlrd import open_workbook
        
        headers = ['year','dest','origin','status']
        
        for k, v in self.metadata.sources.items():
        
            if not k.startswith('trans'):
                continue
        
            f = self.filesystem.download(k)
            wb = open_workbook(f)
            
            y = int(k.replace('trans',''))
            
            if k in ['trans2008','trans2009','trans2010']:
                # These files have a worksheet per destination school
                for sheet in  wb.sheets():
                    dest_school = sheet.name

                    for i,row in enumerate(range(sheet.nrows)):
                        row = [ sheet.cell(row,col).value for col in range(sheet.ncols) ]
                    
                        if len(row) > 1 and row[1].lower().strip() in ('approved','pending','denied'):
                            yield  dict(zip(headers, (y, dest_school, row[0], row[1])))
                       
            else:
                # Sensible files, with source and dest school on each line. 
                sheet = [s for s  in  wb.sheets()][0]
       
                for i,row in enumerate(range(sheet.nrows)):
                    row = [ sheet.cell(row,col).value for col in range(sheet.ncols) ]
                
                    try:
                        if len(row) > 2 and row[2].lower().strip() in ('approved','pending','denied'):
                            yield  dict(zip(headers, (y,) + tuple(row)))
                    except AttributeError:
                        # One row has a date rather than 'approved', etc
                        self.error("Failed for row {}, file {} : {}".format(i, f, row))
                        
                        
                  
    def build_load_crosswalk(self):
        import csv
        from ambry.dbexceptions import QueryError

        fn = self.filesystem.download('code_cross')

        p = self.partitions.find_or_new(table='swtransfers')
        try:
            p.query('DELETE FROM code_cross')
        except QueryError:
            pass
            
        with open(fn) as f:
            reader = csv.DictReader(f)
            
            with p.inserter('code_cross') as ins:
                for row in reader:
                   
                    ins.insert(row)
               

    def build_transfers(self):
        
        p = self.partitions.find_or_new(table='swtransfers')
        p.clean()

        with p.inserter() as ins:
            for row in self.gen_rows():
                ins.insert(row)
                
        return True
        
    def build(self):
        
        self.build_transfers()
        
        self.build_load_crosswalk()
        
        return True
        
                
            

