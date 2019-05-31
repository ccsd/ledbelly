;WITH daily_users AS (
  -- each user by each date
  SELECT DISTINCT
    CONVERT(date, event_time_local) AS date
    , user_id_meta
  FROM live_stream
), daily_active_users AS (
  -- count the number of users for each date
  SELECT
    date
    , COUNT(user_id_meta) AS dau
  FROM daily_users
  GROUP BY
    date	
), daily_avg_users AS (
  -- 7 day rolling average users per day
  SELECT
    date
    , AVG(dau) OVER (ORDER BY date ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_dau
  FROM daily_active_users
)

SELECT 
  dau.date
  , DATEPART(dw, dau.date) week_day
  , dau.dau
  , avg_dau.avg_dau
FROM daily_active_users dau
  JOIN daily_avg_users avg_dau ON avg_dau.date = dau.date
  WHERE dau.date >= GETDATE()-30
ORDER BY dau.date