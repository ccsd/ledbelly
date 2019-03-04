-- events processed within the last 60 seconds
SELECT 
    event_name, 
    MAX(processed_at) last_processed_at, 
    MAX(event_time_local) last_event_time_local, 
    COUNT(*) messages 
FROM live_stream 
WHERE processed_at >= DATEADD( minute, -1, GETDATE()) 
GROUP BY 
    event_name 
ORDER BY 
    last_processed_at DESC;