DROP TABLE IF EXISTS k3_result;
CREATE TABLE k3_result (
  substr text,
  sum double precision
);

COPY k3_result FROM '/results/q2_1.csv' WITH DELIMITER '|';
COPY k3_result FROM '/results/q2_2.csv' WITH DELIMITER '|';
