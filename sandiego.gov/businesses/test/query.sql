attach database '../build/sandiego.gov/businesses-orig-1.0.3/businesses.db' as b;
attach database '../build/sandiego.gov/businesses-orig-1.0.3/ck_addresses.db' as  ck;
attach database '../build/sandiego.gov/businesses-orig-1.0.3/dstk_addresses.db' as dstk;

DROP VIEW IF EXISTS cb;


SELECT
cka.confidence AS cka_score,
cka.ndist AS ndist,
dstka.confidence as dstk_score,
businesses.id, 
cka.parsed_addr,
cka.lat/100000000.0 as lt1,
cka.lon/100000000.0 as ln1,
dstka.lat as lt2,
dstka.lon as ln2

FROM b.businesses  as businesses
LEFT JOIN ck.ck_addresses AS cka ON businesses.id = cka.businesses_id
LEFT JOIN dstk.dstk_addresses as dstka ON businesses.id = dstka.businesses_id
;