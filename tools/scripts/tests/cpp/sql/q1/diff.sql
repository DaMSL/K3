(TABLE pg_result EXCEPT TABLE k3_result) UNION ALL (TABLE k3_result EXCEPT TABLE pg_result);
