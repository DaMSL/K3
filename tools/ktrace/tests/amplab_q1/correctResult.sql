DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (
  pageURL text,
  pageRank int,
  avgDuration int
);
\copy rankings from data/rankings1 with delimiter ',';
\copy rankings from data/rankings2 with delimiter ',';
DELETE FROM CorrectResults;
INSERT INTO CorrectResults (pageURL, pageRank)
  SELECT pageURL, pageRank
  FROM rankings
  WHERE pageRank > 1000;
