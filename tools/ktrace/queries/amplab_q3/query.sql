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
