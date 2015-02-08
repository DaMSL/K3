SELECT p.substr, p.sum FROM pg_result p, k3_result k WHERE p.substr = k.substr and ABS(p.sum - k.sum) > .01
UNION ALL
SELECT * FROM pg_result p WHERE NOT EXISTS (select * from k3_result k where p.substr = k.substr)
UNION ALL
SELECT * FROM k3_result k WHERE NOT EXISTS (select * from pg_result p where p.substr = k.substr);

