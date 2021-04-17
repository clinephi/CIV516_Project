/* GET FINAL METRICS FOR CIV516 */ 
DECLARE target_pattern STRING DEFAULT '52G'; /* INPUT THE BRANCH TO ANALYZE */ 

/* 1.0 - AVERAGE OVERALL OPERATING SPEED */ 
SELECT 
    AVG( operating_speed_kph ) 
FROM 
    ts2.oneway_line_operating_speeds
; 
/* 1.1 - STRATIFY BY DAY TYPE AND TIME PERIOD */ 
SELECT 
    TimePeriod_FromStop, day_type, AVG( operating_speed_kph ) AS avg_op_speed
FROM 
    `cio-insights-covid-ds13-pr-b0.ts2.oneway_line_operating_speeds_with_dt`
WHERE 
    operating_speed_kph IS NOT NULL 
    AND 
    operating_speed_kph > 0 
GROUP BY 
    TimePeriod_FromStop, day_type
ORDER BY    
    day_type, TimePeriod_FromStop
; 

/* 2.0 - OVERALL STOP TO STOP SEGMENT AVERAGE OPERATING SPEED */ 
SELECT 
    AVG(stop2stop_speed_kph	 ) AS avg_speed
FROM 
    ts2.stop_to_stop_stats
WHERE 
    stop2stop_speed_kph IS NOT NULL AND stop2stop_speed_kph > 0 
;
/* 2.1 - AVERAGE STOP TO STOP SEGMENT OPERATING SPEED */ 
SELECT 
    DepartureStop, ArrivalStop, AVG(stop2stop_speed_kph	 ) AS avg_speed
FROM 
    ts2.stop_to_stop_stats
WHERE 
    stop2stop_speed_kph IS NOT NULL AND stop2stop_speed_kph > 0 
GROUP BY   
    DepartureStop, ArrivalStop 
ORDER BY 
    avg_speed
;

/* 3.0 - Overall On time performance */ 
DROP TABLE IF EXISTS ts2.stop_otp ; 
CREATE TABLE ts2.stop_otp AS 
SELECT 
  avl.DepartureStop ,arrival_adherence_grade, COUNT(*) AS grade_count, ANY_VALUE( total_stops_made )  AS count_total, COUNT(*) / ( ANY_VALUE( total_stops_made ) ) * 100 as grade_percent
FROM 
(
    SELECT 
      *, 
      CASE 
        WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -60 AND 180 THEN 'GRADE A' -- 1 min early to 3 min late
        WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -120 AND 360 THEN 'GRADE B' -- 2 min early to 6 min late 
        WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -240 AND 720 THEN 'GRADE C' -- 4 min early to 12 min late
        ELSE 'GRADE D' 
      END AS arrival_adherence_grade 
    FROM 
      ts2.avl_data
    WHERE
        PatternName = target_pattern  
) avl
INNER JOIN 
( 
    SELECT 
        DepartureStop, COUNT(*) AS total_stops_made
    FROM 
        ts2.avl_data 
    GROUP BY 
        DepartureStop
) totals 
ON 
avl.DepartureStop = totals.DepartureStop
GROUP BY 
  DepartureStop, arrival_adherence_grade
;

SELECT 
    *
FROM 
    ts2.stop_otp 
WHERE 
    arrival_adherence_grade = 'GRADE D'
ORDER BY 
    grade_percent DESC




