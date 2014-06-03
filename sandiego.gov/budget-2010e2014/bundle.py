'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        
        for k, _ in self.metadata.sources.items():

            fn = self.filesystem.download(k)
        
            try:
                self.schema.update_csv(k, fn)
            except Exception as e:
                self.error("Failed to create schema for: {}: {}".format(k, e))
        return True
        
