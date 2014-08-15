'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta_scrape_index(self):
        """Scrape the index page to get links to the data files. """
        from bs4 import BeautifulSoup as soup
        import requests
        import os.path
        
        #self.metadata.ensure_loaded('sources')
        
        url = self.metadata.sources.index.url

        r = requests.get(url)
        r.raise_for_status()
        
        table = soup(r.content).find('table')
        
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) == 3:
                
                year = int(cols[0].text.encode('utf8').strip()[:4])
                file_url = cols[1].find('a')['href']
                dd_url = os.path.join(os.path.dirname(url), cols[2].find('a')['href'])
                self.metadata.sources.get(year).url = file_url
                self.metadata.sources.get(year).dd_url = dd_url 
                self.log("Added {}: {}".format(year, url))
        
        self.metadata.write_to_dir()
        
    @property
    def schema_map(self):
        """Map schema URLS to table names"""
        from os.path import basename
        
        schema_urls = set()
        
        d = {}
        
        for k, v in self.metadata.sources.items():
            try:
                int(k)
            except: # All of the year data files have names that are ints
                continue
                
            schema_urls.add(v.dd_url)
            
        for schema_url in schema_urls:

            d[schema_url] = dict(
                fsenr = 'enroll09',
                fsenr98 = 'enroll98',
                fsenr93 = 'enroll93',
            )[basename(schema_url).replace('.asp','')]
            
        return d
            
    def meta_scrape_schemas(self):
        
        self.prepare()
        
        type_map = dict(
            Character = 'varchar',
            Numeric = 'integer'
        )
        
        for k, v in self.schema_map.items():
        
            t = self.schema.add_table(v)
            t.add_column('id', datatype='integer', is_primary_key = True)
            t.add_column('year', datatype='integer')
        
            for row in self.meta_scrape_schema(k):

                t.add_column(row['Field Name'], width = row['Width'], datatype=type_map[row['Type Field']],
                            description = row['Description'])
            
            
        self.schema.write_schema()


    def meta_scrape_schema(self, url):
        from bs4 import BeautifulSoup as soup
        import requests
        import os.path
  
        self.log(url)
  
        r = requests.get(url)
        r.raise_for_status()
        
        table = soup(r.content).find('table')
        
        for i,row in enumerate(table.find_all('tr')):

            if i == 0:
                header = [c.text.strip() for c in row.find_all('th')]
            else:
                row =  [c.text.strip() for c in row.find_all('td')]
            
                yield dict(zip(header, row))

    def gen_rows(self, table_name):
        import csv
        
        self.log("Gen: {}".format(table_name))
        
        smap = self.schema_map
        
        for k, v in self.metadata.sources.items():
            
            if k == 'index':
                continue
            
            if table_name != smap[v.dd_url]:
                continue

                
            fn = self.filesystem.download(k)
            
            with open(fn) as f:
                
                reader = csv.DictReader(f, dialect='excel-tab')
                
                for row in reader:
                    row['year'] = k
                    yield  row
        

    def build(self):
        
        lr = self.init_log_rate(20000)
        
        for table in self.schema_map.values():
            p = self.partitions.find_or_new(table = table)
            p.clean()
            
            with p.inserter() as ins:
                for row in self.gen_rows(table):
                    lr(table)
                    ins.insert(row)
                    
        return True
        

