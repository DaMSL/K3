DROP TABLE IF EXISTS k3_result_dp CASCADE;
CREATE TABLE k3_result_dp (
  sourceIP text,
  totalRevenue double precision,
  avgPageRank double precision
);

-- Only the master peer produces a result for this query
COPY k3_result_dp FROM '/results/q3_1.csv' WITH DELIMITER '|';

DROP VIEW IF EXISTS k3_result;
DROP TABLE IF EXISTS k3_result;
CREATE VIEW k3_result AS
SELECT sourceIP, ROUND(totalRevenue::numeric, 2) as totalRevenue, ROUND(avgPageRank::numeric, 2) as avgPageRank
FROM k3_result_dp;
