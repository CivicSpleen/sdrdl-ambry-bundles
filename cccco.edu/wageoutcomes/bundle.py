'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle

class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    #
    # The following  are custom caster types. They are linked 
    # to fields in the d_caster column of the schema. These can be derived
    # functions, or derived from types. 
    #
    # The only reason there is one function and one class is an an example of each. 
    # 

    @staticmethod
    def percent(v):
        return int(float(v.strip('%')))

    class dollars(int):
        def __new__(self, v):
            return int.__new__(self, float(v.strip("$").replace(',','')))


    def header(self, name, row1, row2):
        
         headertrans = self.filesystem.read_yaml('meta','orig-header.yaml') 
        
         orig_header = [ ', '.join(a).strip(', ') for a in  zip(row1, row2) ]
         
         # Uncomment to write the header translator. 
         #self.filesystem.write_yaml(header,'meta','orig-header-2.yaml')

         trans = { k:v for k,v in headertrans[name] }

         return [ trans[h] for h in orig_header]

    def meta(self):
        import csv
        
        self.database.create()
        self._prepare_load_schema()

        for name, source in self.metadata.sources.items():

            csv_file = self.filesystem.download(source.url)
            
            self.log("Build schema for {} using {}".format(name, source.url))
            
            with open(csv_file) as f:
                r = csv.reader(f)
                row1 = r.next()
                row2 = r.next()
                header = self.header(name, row1, row2)

                def itr():
                    for i, row in enumerate(r):
                        if i > 100:
                            break
        
                        yield dict(zip(header, row))

    
                self.schema.update(name, itr())
            
        return True

    def build(self):
        import csv
        
        for name, source in self.metadata.sources.items():

            csv_file = self.filesystem.download(source.url)
            
            self.log("Building  {} using {}".format(name, source.url))
            
            p = self.partitions.find_or_new(table=name)
            p.clean()
            
            with open(csv_file) as f:
                r = csv.reader(f)
                    
                row1 = r.next()
                row2 = r.next()
                header = self.header(name, row1, row2)

                with p.inserter() as ins:
                    for i, row in enumerate(r):
                        ins.insert(dict(zip(header, row)))
        
        return True

