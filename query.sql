WITH events AS (

SELECT
  id
FROM `database.bronze.event_history`
WHERE DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1)

SELECT 
  eh.id AS to_eh_id,
  eh.event_id,
  JSON_EXTRACT_ARRAY(eh.values) AS values
FROM `database.bronze.event_history` eh
INNER JOIN events e
  ON eh.id = e.id
WHERE eh.items != '[]'
ORDER BY eh.event_id, id