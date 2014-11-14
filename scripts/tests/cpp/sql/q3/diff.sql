SELECT p.sourceIP, p.totalRevenue, p.avgPageRank  FROM pg_result p, k3_result k WHERE p.sourceIP = k.sourceIP and ( ABS(p.totalRevenue - k.totalRevenue) > .01 or ABS(p.avgPageRank - k.avgPageRank) > .01 ) 
UNION ALL
SELECT * FROM pg_result p WHERE NOT EXISTS (select * from k3_result k where p.sourceIP = k.sourceIP)
UNION ALL
SELECT * FROM k3_result k WHERE NOT EXISTS (select * from pg_result p where p.sourceIP = k.sourceIP);

