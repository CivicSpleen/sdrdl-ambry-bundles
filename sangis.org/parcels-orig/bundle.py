'''
'''

from  ambry.bundle.geo import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''

    def build(self):
        from ambry.identity import PartitionNameQuery
        
        super(Bundle, self ).build()

        p = self.partitions.find(PartitionNameQuery(table='parcels'))
        
        pdq = p.database.query
        
        pdq('CREATE INDEX IF NOT EXISTS apn_idx ON parcels (apn, apn_8);')
        pdq('CREATE INDEX IF NOT EXISTS parcel_idx ON parcels (parcelid);')
        pdq('CREATE INDEX IF NOT EXISTS road_idx ON parcels (addrno, roadname);')
        pdq('CREATE INDEX IF NOT EXISTS sroad_idx ON parcels (situs_addr, situs_stre);')
        pdq('CREATE INDEX IF NOT EXISTS zip_idx ON parcels (zip);')
        pdq('CREATE INDEX IF NOT EXISTS szip_idx ON parcels (situs_zip);')
        pdq('CREATE INDEX IF NOT EXISTS jur_idx ON parcels (situs_juri);')
        pdq('CREATE INDEX IF NOT EXISTS coord_idx ON parcels (x_coord, y_coord);')

        return True
