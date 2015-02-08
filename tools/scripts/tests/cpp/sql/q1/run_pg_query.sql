CREATE TABLE rankings (
  pageURL text,
  pageRank int,
  avgDuration int
);

COPY rankings FROM '/local/data/amplab/1024/rankings/rankings0000' WITH DELIMITER ',';
COPY rankings FROM '/local/data/amplab/1024/rankings/rankings0001' WITH DELIMITER ',';

CREATE VIEW pg_result as
SELECT pageURL, pageRank
FROM rankings
WHERE pageRank > 1000;
