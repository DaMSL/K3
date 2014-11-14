DROP TABLE IF EXISTS uservisits CASCADE;
CREATE TABLE uservisits (
  sourceIP text,
  destURL text,
  visitDate text,
  adRevenue double precision,
  userAgent text,
  countyCode text,
  languageCode text,
  searchWord text,
  duration int
);

COPY uservisits FROM '/local/data/amplab/1024/uservisits/uservisits0000' WITH DELIMITER ',';
COPY uservisits FROM '/local/data/amplab/1024/uservisits/uservisits0001' WITH DELIMITER ',';

DROP VIEW IF EXISTS pg_result;
CREATE VIEW pg_result as
SELECT substring(sourceIP from 1 for 8) as substr, ROUND(SUM(adRevenue)::numeric, 2) as sum
FROM uservisits
GROUP BY substring(sourceIP from 1 for 8)
