CREATE TABLE k3_result (
  pageURL text,
  pageRank int
);

COPY k3_result FROM '/results/q1_1.csv' WITH DELIMITER ',';
COPY k3_result FROM '/results/q1_2.csv' WITH DELIMITER ',';
