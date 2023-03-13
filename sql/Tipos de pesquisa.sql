SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '1 month'
    AND lm.name = l.name 
GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '1' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '1 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '1' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '1 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '1' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '1 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '1' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '1 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '1 month'
    GROUP BY location_name
UNION ALL
SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '2 month'
    AND lm.name = l.name 
GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '2' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '2 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '2' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '2 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '2' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '2 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '2' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '2 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '2' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '2 month'
    GROUP BY location_name
UNION ALL
SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '3 month'
    AND lm.name = l.name 
GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '3' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '3 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '3' AS "month"
FROM location_metrics lm, "location" l
WHERE lm.start_time >= now() - interval '3 month'
    AND lm.name = l.name
    GROUP BY l.location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '3' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '3 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '3' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '3 month'
    GROUP BY location_name
UNION ALL
SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '3' AS "month"
FROM location_metrics lm
WHERE lm.start_time >= now() - interval '3 month'
    GROUP BY location_name
    