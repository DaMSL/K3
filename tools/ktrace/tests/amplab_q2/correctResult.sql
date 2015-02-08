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
INSERT INTO CorrectResults (substr, sum) 
  SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 8);
