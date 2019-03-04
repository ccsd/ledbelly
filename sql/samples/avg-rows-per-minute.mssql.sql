-- average rows per minute
SELECT 
    AVG(a.total_per) 
FROM (
	SELECT 
          DATEADD(minute, DATEDIFF(minute, 0, processed_at), 0) AS day_minute, 
          Count(*) AS total_per
 	FROM live_stream
 	GROUP BY 
          DATEADD(minute, DatDATEDIFFeDiff(minute, 0, processed_at), 0)
  ) a