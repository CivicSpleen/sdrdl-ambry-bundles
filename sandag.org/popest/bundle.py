'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 


class Bundle(ExcelBuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_add_descriptions(self):
        """The second workskeet of the data file has the descriptions for each column. Load these and alter the
        schema to include them. """
        from xlrd import open_workbook
        
        fn, sheet_num = self.get_wb_sheet('descriptions')

        descriptions = {}

        with open(fn) as f:

            wb = open_workbook(fn)

            s = wb.sheets()[sheet_num]

            for i, row in enumerate(range(1,s.nrows)):
                d =  self.srow_to_list(row, s)
                descriptions[d[0]] = d[1]
                
        with self.session:
            
            for c in self.schema.table('forecast13').columns:
                c.description = descriptions.get(c.name, '')
                
        self.schema.write_schema()
            
            
    def meta(self):
        
        super(Bundle, self).meta()
        
        self.meta_add_descriptions()
        
        return True


