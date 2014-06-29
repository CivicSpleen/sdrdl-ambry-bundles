'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def clean_wsname(self, name):
        
        return ''.join(ch for ch in name if ch.isalnum() or ch == ' ').strip().replace(' ','_').lower()

    def transform_heading(self, values):
        
        return  [ v.strip() if v.strip() else str(i) for i,v in enumerate(values)]

    def meta_scan_headers(self):
        """The headers in these files are a complete wreck. This produces a mapping from columns or 
        existing header to sane headers. The file is handed editied and then reloaded
        on subsequent invocations.  """
        from xlrd import open_workbook
        
        try:
            headings = self.filesystem.read_yaml('meta','headings.yaml')
            
        except IOError:
            headings = None
            
        if not headings:
            headings = {}
            
        
        file_map = [] # Map the key and ws names to the original files. 
            
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)
            
            wb = open_workbook(f)

            if not k in headings: # Yaml doesn't like defaultdict
                headings[k] = {}

            self.log("Key: {} File: {}".format(k, f))

            for i, s in enumerate(wb.sheets()):
                
                wsname = self.clean_wsname(s.name)

                if not wsname.strip():
                    wsname = str(i)
                
                self.log("    Worksheet: {}".format(wsname))

                file_map.append([k, wsname, f, s.name])

                for i,row in enumerate(range(s.nrows)):
                    values = []
                    for col in range(s.ncols):
                        values.append(s.cell(row,col).value)

                    if i == 0:
                        
                        if not wsname in headings[k]:
                            headings[k][wsname] = []
                            
                        for orig_heading in self.transform_heading(values):
                            if not orig_heading in headings[k][wsname]:
                                headings[k][wsname].append([orig_heading,''])
                        
                    else:
                        self.log(', '.join([ str(v) for v in values]))
                    
                    if i > 5:
                        break
                    
        self.filesystem.write_yaml(headings, 'meta','headings.yaml')
             
        with open(self.filesystem.path('meta','filemap.csv'), 'w+') as f:
            import csv
            writer = csv.writer(f)
            writer.writerows(file_map)
          
    def meta_skips_list(self):
        """Create a file to record how many header lines should be skipped in each sheet"""
        
        try:
            skips = self.filesystem.read_yaml('meta','skips.yaml')
            
        except IOError:
            skips = None
            
        if not skips: 
            skips = {}
                    
        headings = self.filesystem.read_yaml('meta','headings.yaml')

        for file_key, sheets in headings.items():
            for sheet_name, header in sheets.items():
                k = file_key+';'+sheet_name

                if not k in skips:
                    skips[k] = 1
          
          
        self.filesystem.write_yaml(skips, 'meta','skips.yaml')  
          
    def meta_gen_schema(self):
        
        
        self.database.create()
        
        headings = self.filesystem.read_yaml('meta','headings.yaml')
        
        for file_key, sheets in headings.items():
            for sheet_name, header_map in sheets.items():
                
                header = [ i[1] for i in header_map ]
                
                print '====', file_key, sheet_name
                print '    ', header
  
                self.schema.update(sheet_name, self.gen_rows(file_key, sheet_name), 
                        n=2000, header = header, logger = self.init_log_rate(500))
                
            
            
        self.schema.write_schema()
    
            
    def gen_rows(self,file_key, sheet_name, accumulate=False):
        '''If accumulate is true, each new row is overlaid on the previous row, to fill in 
        all the elements in the heirarchy. '''
        from xlrd import open_workbook
        
        f = self.filesystem.download(file_key)
        
        wb = open_workbook(f)
        
        sheet = None
        
        for test_sheet in wb.sheets():
            
            if self.clean_wsname(test_sheet.name) == sheet_name:
                sheet = test_sheet
                break
        
        assert sheet != None
        
        past_header = False
        
        last_values = None
        
        #headings = self.filesystem.read_yaml('meta','headings.yaml') 
        #header = [ i[1] for i in headings[file_key][self.clean_wsname(sheet.name)]]
        
        skips = self.filesystem.read_yaml('meta','skips.yaml')
        
        skip_lines = skips[file_key+';'+sheet_name]
        
        for i,row in enumerate(range(sheet.nrows)):
            
            if i <= skip_lines:
                continue
            
            values = []
            for col in range(sheet.ncols):
                values.append(sheet.cell(row,col).value)
 
            if accumulate:
                
                if not last_values:
                    last_values = [None]*len(values)
                
                for i,v in enumerate(values):
                    last_values[i] = values[i] if values[i] else last_values[i] 
                values = last_values
                    
            yield values
            
            last_values = values
                  
    def meta(self):

        self.meta_scan_headers()
        
        self.meta_gen_schema()
        
    def build(self):
        
        headings = self.filesystem.read_yaml('meta','headings.yaml')
        
        p = self.partitions.find_or_new(table='fmdatacip', tables = [ t.name for t in self.schema.tables])
        
        for file_key, sheets in headings.items():
            for sheet_name, header_map in sheets.items():
                
                print '====', file_key, sheet_name
                
                
                lr = self.init_log_rate(2000)
                
                header = [ i[1] for i in header_map ]
                
                accumulate = False
                
                if sheet_name in ('fchierarchy', 'pbf_publishing_commitment_item'):
                    accumulate = True
                
                with p.inserter(sheet_name) as ins:
                    for i, row in enumerate(self.gen_rows(file_key, sheet_name, accumulate = accumulate)):
                        lr(sheet_name)
                        ins.insert(dict(zip(header, row)))
                        
                      
        return True
                        
                