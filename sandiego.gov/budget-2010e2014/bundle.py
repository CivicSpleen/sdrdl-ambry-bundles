'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        from xlrd import open_workbook
        
        for k in self.metadata.sources.keys():
            f = self.filesystem.download(k)
            
            self.log("Processing: {}".format(f))
            wb = open_workbook(f)

            for s in wb.sheets():
                self.log("    Worksheet: {}".format(s.name))
                continue
                for i,row in enumerate(range(s.nrows)):
                    values = []
                    for col in range(s.ncols):
                        values.append(s.cell(row,col).value)

                    print values
        
