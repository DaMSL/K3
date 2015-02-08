DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (
  pageURL text,
  pageRank int,
  avgDuration int
);
\copy rankings from data/rankings1 with delimiter ',';
\copy rankings from data/rankings2 with delimiter ',';

DROP TABLE IF EXISTS uservisits;
CREATE TABLE uservisits (
  sourceIP text,
  destURL text,
  visitDate text,
  adRevenue double precision,
  userAgent text,
  countryCode text,
  languageCode text,
  searchWord text,
  duration int
);
\copy uservisits from data/uservisits1 with delimiter ',';
\copy uservisits from data/uservisits2 with delimiter ',';

DELETE FROM CorrectResults;
INSERT INTO CorrectResults (sourceIP, totalRevenue, avgPageRank)
  SELECT sourceIP, totalRevenue, avgPageRank
  FROM
    (SELECT sourceIP,
            AVG(pageRank) as avgPageRank,
            SUM(adRevenue) as totalRevenue
      FROM Rankings AS R, UserVisits AS UV
      WHERE R.pageURL = UV.destURL
         AND UV.visitDate >= '1980-01-01' 
	 AND UV.visitDate <= '1980-04-01' 
      GROUP BY UV.sourceIP) as T
    ORDER BY totalRevenue DESC LIMIT 1;
