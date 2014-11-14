DROP TABLE IF EXISTS k3_result_dp CASCADE;
CREATE TABLE k3_result_dp (
  substr text,
  sum double precision
);

COPY k3_result_dp FROM '/results/q2_1.csv' WITH DELIMITER '|';
COPY k3_result_dp FROM '/results/q2_2.csv' WITH DELIMITER '|';

DROP VIEW IF EXISTS k3_result;
DROP TABLE IF EXISTS k3_result;
CREATE VIEW k3_result AS
SELECT substr, ROUND(sum::numeric, 2) as sum
FROM k3_result_dp;
