 WITH CTE AS ( (
                    SELECT city, CAST(time AS DATE) AS day, temperature_c as temp
                    FROM huge-challenge.weather.predictions a WHERE
                    a.measure_ts = (
                        SELECT MAX(b.measure_ts)
                        FROM huge-challenge.weather.predictions b
                        WHERE b.city = a.city AND b.time = a.time) ) )
                SELECT city, DAY, AVG(temp) AS avg_temp, MAX(temp) AS min_temp, MIN(temp) AS max_temp
                FROM CTE GROUP BY CITY, DAY ; 