
# {about_title}

{about_summary}


The JSON data is stored encoded un UTF-8 and compressed with ZLIB. To decode, use Python code like this: 


        projects = self.library.dep('projects').partition
        
        for row in projects.rows:
            
            print str(row.data).decode('zlib').decode('utf8')
