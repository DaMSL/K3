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

DROP TABLE IF EXISTS rankings CASCADE;
CREATE TABLE rankings (
  pageURL text,
  pageRank int,
  avgDuration int
);


COPY rankings FROM '/local/data/amplab/1024/rankings/rankings0000' WITH DELIMITER ',';
COPY rankings FROM '/local/data/amplab/1024/rankings/rankings0001' WITH DELIMITER ',';

DROP VIEW IF EXISTS pg_result;
CREATE VIEW pg_result AS
SELECT sourceIP, ROUND(totalRevenue::numeric, 2) as totalRevenue, ROUND(avgPageRank::numeric, 2) as avgPageRank
FROM
  (SELECT sourceIP, AVG(pageRank) as avgPageRank, SUM(adRevenue) as totalRevenue
   FROM Rankings AS R, UserVisits AS UV
   WHERE R.pageURL = UV.destURL
      AND UV.visitDate BETWEEN '1980-01-01' AND '1980-04-01'
   GROUP BY UV.sourceIP) t
ORDER BY totalRevenue DESC LIMIT 1
