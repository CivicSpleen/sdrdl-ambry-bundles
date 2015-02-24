from  ambry.bundle.loader import CsvBundle
 

class Bundle(CsvBundle):

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        
        super(Bundle, self).build()
        
        self.deident()
        
        return True


    def deident(self):
        
        referrals = self.partitions.find(table='referrals')
        
        t_map = self.t_map()
        
        students_s = set()
     
        for row in referrals.rows:
            row = dict(row)
            students_s.add(str(row['first_name'])+str(row['last_name']))
        
        students = {}
        teachers = {}

        for i,r in enumerate(students_s, start=1):
            students[r] = i
            
        p = self.partitions.find_or_new(table='direferrals')
        p.clean()

        with p.inserter() as ins:
            for row in referrals.rows:
                row = dict(row)
                
                row['student_id'] = students[str(row['first_name'])+str(row['last_name'])]
                
                row['documenting_staff'] = t_map[row['documenting_staff_member']][0]
                row['documenting_staff_id'] = t_map[row['documenting_staff_member']][1]
                row['reporting_staff'] = t_map[row['reporting_staff_member']][0]
                row['reporting_staff_id'] = t_map[row['reporting_staff_member']][1]
           
                
                ins.insert(row)
                
    def t_map(self):
        import csv
        
        fn = self.filesystem.path('meta','teacher_map.csv')
        
        t_map = {}
        
        with open(fn) as f:
            r = csv.DictReader(f)
            
            for row in r:
                t_map[row['orig_name']] = row['mapped_name']
                t_map[row['mapped_name']] = row['mapped_name']
            
            
        t_map[None] = ''

        for i, key in enumerate(sorted(t_map.keys())):
            t_map[key] = (t_map[key], i)
        
        
        return t_map
        
        
        
                
                