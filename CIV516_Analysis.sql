/* CIV516 PROJECT WORK */ 
/* FIND the minimum and maximum stop sequences by trip */
DECLARE target_pattern STRING DEFAULT '52F'; /* INPUT THE BRANCH TO ANALYZE */ 
/* CREATE A TABLE OF THE START AND ENDS OF AVL TRIPS */
/* PROCESS DESCRIPTION 
  1. Get the max and min stop sequences for each trip and day in the AVL data 
  2. Filter only these max and min records in the AVL data
  3. Join the shape_dist_travelled fields to both types of records
  4. LEAD on the min_ss to get the max_ss sdt and other parameters
  5. Filter only min_ss 
*/ 
DROP TABLE IF EXISTS ts2.route52_startends; 
CREATE TABLE ts2.route52_startends AS
SELECT 
  *
FROM 
  ( 
SELECT 
  *, 
  LEAD( ActualTime_ArrivalStop, 1 ) OVER ( PARTITION BY Date_Key, TripId ORDER BY StopSequence ) AS final_arrival_time,  --Grab the end stop 
  LEAD( 
      arrival_stop_sdt, 1 ) 
  OVER(
    PARTITION BY Date_Key, TripId ORDER BY StopSequence
  ) AS final_sdt,
  LEAD( 
      StopSequence, 1 ) 
  OVER(
    PARTITION BY Date_Key, TripId ORDER BY StopSequence
  ) AS final_ss,
  LEAD( ArrivalStop, 1 ) OVER ( PARTITION BY Date_Key, TripId ORDER BY StopSequence ) AS final_stop_name,  
  LEAD( ActualTime_ArrivalStop, 1 ) OVER ( PARTITION BY Date_Key, TripId ORDER BY StopSequence ) AS final_stop_arrival_time
FROM 
  (
    SELECT 
      avl.*, st1.shape_dist_traveled AS departure_stop_sdt, st2.shape_dist_traveled AS arrival_stop_sdt
    FROM 
      (
        SELECT 
          avl.*, mm.max_ss, mm.min_ss 
        FROM 
          ts2.avl_data avl
          INNER JOIN 
          ( 
            SELECT 
              Date_Key, TripId, MAX( StopSequence ) AS max_ss, MIN( StopSequence ) AS min_ss 
            FROM 
              ts2.avl_data 
            WHERE 
              PatternName = target_pattern
            GROUP BY 
             Date_Key, TripId
          ) mm 
          ON 
          avl.Date_Key = mm.Date_Key
          AND 
          avl.TripId = mm.TripId 
          AND 
          (avl.StopSequence = mm.max_ss OR avl.StopSequence = mm.min_ss ) 
      ) avl 
      INNER JOIN 
      may2018_gtfs.stops s1
      ON 
      CAST(avl.DepartureStopNumber AS STRING) = s1.stop_code
      INNER JOIN 
      may2018_gtfs.stops s2
      ON 
      CAST(avl.ArrivalStopNumber AS STRING) = s2.stop_code  --this might run into issues 
      INNER JOIN 
      may2018_gtfs.stop_times st1 
      ON 
      s1.stop_id = CAST(st1.stop_id AS INT64)
      AND 
      avl.TripId = CAST(st1.trip_id AS INT64)
      INNER JOIN 
      may2018_gtfs.stop_times st2
      ON 
      s2.stop_id = CAST(st2.stop_id AS INT64) 
      AND 
      avl.TripId  = CAST(st2.trip_id AS INT64) 
  )
  )
WHERE 
  StopSequence = min_ss
;

/* CREATE A TABLE OF OPERATING SPEEDS, TO SLICE AND DICE LATER */ 
DROP TABLE IF EXISTS ts2.oneway_line_operating_speeds; 
CREATE TABLE ts2.oneway_line_operating_speeds AS 
SELECT 
  *,
  (final_sdt - departure_stop_sdt) * 1000 AS operating_dist_m,
  TIME_DIFF( 
    EXTRACT( TIME FROM final_stop_arrival_time) , EXTRACT( TIME FROM ActualTime_DepartureStop) , SECOND
  ) AS operating_time_s, 
  CASE 
    WHEN TIME_DIFF( EXTRACT( TIME FROM final_stop_arrival_time), EXTRACT( TIME FROM ActualTime_DepartureStop), SECOND) = 0 THEN NULL 
    ELSE ( final_sdt - departure_stop_sdt) * 1000 / TIME_DIFF( EXTRACT( TIME FROM final_stop_arrival_time), EXTRACT( TIME FROM ActualTime_DepartureStop), SECOND) * 3.6 
  END AS operating_speed_kph 
FROM 
  ts2.route52_startends
;

/* SLICE AND DICE ONE WAY LINE OPERATING SPEEDS */
/* 1.0 Overall Average Operating Speed */ 
SELECT 
  COUNT(*) 
FROM 
  ts2.oneway_line_operating_speeds
WHERE 
  operating_speed_kph IS NOT NULL 
  AND 
  operating_speed_kph > 0 
; --RESULT: 17.988
SELECT 
  avg( operating_speed_kph ) AS overall_avg_operating_speed_kph
FROM 
  ts2.oneway_line_operating_speeds
WHERE 
  operating_speed_kph IS NOT NULL 
  AND 
  operating_speed_kph > 0 
; --RESULT: 17.988

SELECT * FROM ts2.oneway_line_operating_speeds LIMIT 5; 

/* 2.0 Average operating speed by start and end */ 
SELECT 
  DepartureStop, final_stop_name, COUNT(*) AS sample_size, AVG( operating_dist_m ) AS avg_operating_dist_m , AVG( operating_speed_kph ) AS avg_operating_speed_kph , AVG( operating_time_s ) AS avg_operating_time_s
FROM 
  ts2.oneway_line_operating_speeds
GROUP BY 
  DepartureStop, final_stop_name	
ORDER BY 
  AVG( operating_speed_kph )
;

/* 3.0 Stop to Stop Level Analysis */ 
DROP TABLE IF EXISTS ts2.stop_to_stop_stats; 
CREATE TABLE ts2.stop_to_stop_stats AS 
SELECT 
  *
FROM 
( 
SELECT 
  *, 
  CASE 
    WHEN raw_travel_time_s = 0 THEN NULL 
    ELSE sdt_diff_m / raw_travel_time_s * 3.6 
  END AS stop2stop_speed_kph 
FROM 
  (
      SELECT  
        avl.*, st1.shape_dist_traveled AS departure_sdt, st2.shape_dist_traveled AS arrival_sdt, ( st2.shape_dist_traveled - st1.shape_dist_traveled ) * 1000 AS sdt_diff_m,
        /* CALCULATE TRAVEL TIME -- THINGS GET COMPLICATED HERE */
        TIME_DIFF( EXTRACT( TIME FROM ActualTime_ArrivalStop ) , EXTRACT( TIME FROM ActualTime_DepartureStop ), SECOND ) AS raw_travel_time_s
      FROM
        ts2.avl_data avl
        INNER JOIN 
        may2018_gtfs.stops s1 
        ON 
        CAST(avl.DepartureStopNumber AS STRING ) = s1.stop_code
        INNER JOIN 
        may2018_gtfs.stops s2
        ON 
        CAST( avl.ArrivalStopNumber AS STRING ) =  s2.stop_code 
        INNER JOIN 
        may2018_gtfs.stop_times st1 
        ON 
        avl.TripId = CAST( st1.trip_id AS INT64  ) 
        AND 
        s1.stop_id = CAST( st1.stop_id AS INT64  ) 
        INNER JOIN 
        may2018_gtfs.stop_times st2 
        ON 
        avl.TripId = CAST( st2.trip_id AS INT64 ) 
        AND 
        s2.stop_id = CAST( st2.stop_id AS INT64 ) 
  )
) 
WHERE 
  stop2stop_speed_kph IS NOT NULL 
;
 
/* 4.0 On Time Performance */ 
-- Each row in the avl data represents a departure and arrival, which may adhere to the schedule. Let's summarise these overall.  
--Apply a bin label based on the scheduled adherence. 
SELECT 
  d_grades.departure_adherence_grade AS adherence_grade, 
  (d_grades.grade_count + a_grades.grade_count ) / (d_grades.count_total + a_grades.count_total ) * 100.0 AS total_grade_percent, 
  d_grades.grade_percent AS departure_only_perc , 
  a_grades.grade_percent AS arrival_only_perc
FROM 
  (  
      SELECT 
        departure_adherence_grade, COUNT(*) AS grade_count, ( SELECT COUNT(*) FROM ts2.avl_data WHERE PatternName = target_pattern ) AS count_total, COUNT(*) / ( SELECT COUNT(*) FROM ts2.avl_data WHERE PatternName = target_pattern ) * 100 as grade_percent
      FROM 
        ( 
          SELECT 
            *,
            CASE 
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -60 AND 180 THEN 'GRADE A' -- 1 min early to 3 min late
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -120 AND 360 THEN 'GRADE B' -- 2 min early to 6 min late 
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -240 AND 720 THEN 'GRADE C' -- 4 min early to 12 min late
              ELSE 'GRADE D'
            END AS departure_adherence_grade
          FROM 
            ts2.avl_data
          WHERE 
            PatternName =  target_pattern
        ) 
      GROUP BY 
        departure_adherence_grade
  ) d_grades 
  INNER JOIN 
  (
    SELECT 
      arrival_adherence_grade, COUNT(*) AS grade_count, ( SELECT COUNT(*) FROM ts2.avl_data WHERE PatternName = target_pattern ) AS count_total, COUNT(*) / (  SELECT COUNT(*) FROM ts2.avl_data WHERE PatternName = target_pattern ) * 100 as grade_percent
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
      ) 
    GROUP BY 
      arrival_adherence_grade
  ) a_grades 
  ON 
  d_grades.departure_adherence_grade = a_grades.arrival_adherence_grade

;
/* MISC ----- 5.0 ADD DAY TYPE TO THE OPERATING SPEED */
DROP TABLE IF EXISTS ts2.oneway_line_operating_speeds_with_dt; 
CREATE TABLE ts2.oneway_line_operating_speeds_with_dt AS 
SELECT 
  *,
  /* ADD DAY TYPE */
  DATE(  
    CONCAT( 
      LEFT( CAST( Date_Key AS STRING) , 4  ),
      '-',
      SUBSTR( CAST( Date_Key AS STRING), 5, 2 ),
      '-',
      RIGHT( CAST( Date_Key AS STRING), 2 ) 
    )
  ) AS Date_format_key,
  CASE 
    WHEN
        EXTRACT(DAYOFWEEK FROM 
          DATE(  
          CONCAT( 
            LEFT( CAST( Date_Key AS STRING) , 4  ),
            '-',
            SUBSTR( CAST( Date_Key AS STRING), 5, 2 ),
            '-',
            RIGHT( CAST( Date_Key AS STRING), 2 ) 
          )
        ) )  = 1 THEN 'SUNDAY' 
    WHEN 
        EXTRACT(DAYOFWEEK FROM 
          DATE(  
          CONCAT( 
            LEFT( CAST( Date_Key AS STRING) , 4  ),
            '-',
            SUBSTR( CAST( Date_Key AS STRING), 5, 2 ),
            '-',
            RIGHT( CAST( Date_Key AS STRING), 2 ) 
          )
        ) )  IN ( 2, 3, 4, 5, 6 ) THEN 'WEEKDAY' 
     WHEN 
        EXTRACT(DAYOFWEEK FROM 
          DATE(  
          CONCAT( 
            LEFT( CAST( Date_Key AS STRING) , 4  ),
            '-',
            SUBSTR( CAST( Date_Key AS STRING), 5, 2 ),
            '-',
            RIGHT( CAST( Date_Key AS STRING), 2 ) 
          )
        ) ) = 7  THEN 'SATURDAY'
      END AS day_type 
FROM 
  ts2.oneway_line_operating_speeds

