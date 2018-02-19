select dt,
    -- unix time to datetime:
    FROM_UNIXTIME(time_sec) as dt_from_time_sec
FROM (
    -- datetime to unix time:
	select dt, CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) as time_sec
	from ticker
	limit 10
) as test;