### Module 1 Homework: Docker & SQL

<b> Question 1. Understanding docker first run </b>

Run docker with python 3.13:

    CLI Commands used:
    docker run -it --rm --entrypoint=bash python:3.13

Version of pip in the image: 

    CLI Commands used: 
        pip -V
    
    Answer:
        pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)

<b> Question 2. Understanding Docker networking and docker-compose </b>

What is the hostname and port that pgadmin should use to connect to the postgres database?

    Answer: 
        Hostname: postgres
        Port: 5432

        Hostname: db
        Port: 5432

<b> Question 3. Counting short trips </b>

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

    Answer: 
        8,007

    SELECT COUNT(trip_distance) AS Trips_equal_lower_mile

    FROM (SELECT * 
          FROM public.ny_green_taxi_trips
          WHERE lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
          AND trip_distance <= 1
          )
    ;

<b> Question 4. Longest trip for each day </b>

Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

    Answer: 
        2025-11-14
    
    SELECT DATE_TRUNC('DAY',lpep_pickup_datetime) AS PickUpday, 
	       trip_distance AS LongestDistance
	   
    FROM public.ny_green_taxi_trips
    WHERE trip_distance < 100
    AND trip_distance = (SELECT MAX(trip_distance)
                         FROM public.ny_green_taxi_trips
                         WHERE trip_distance < 100
                        )				 
    ;

<b> Question 5. Biggest pickup zone </b>

Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

    Answer: 
        East Harlem North

    SELECT T.*

    FROM (SELECT Z."Zone" AS PUZone, 
                COUNT(*) AS TotalAmount
                
        FROM public.ny_green_taxi_trips T_
        LEFT JOIN public.ny_trip_zones Z
            ON T_."PULocationID" = Z."LocationID"
        WHERE T_.lpep_pickup_datetime BETWEEN '2025-11-18' AND '2025-11-19'
        GROUP BY Z."Zone") T
    ORDER BY TotalAmount DESC
    LIMIT 1
    ;
            
<b> Question 6. Largest tip </b>

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

    Answer: 
        Yorkville West - 81.89

    SELECT T.*

    FROM (SELECT T_."PULocationID", 
                PUZ."Zone" AS PUZone, 
                T_."DOLocationID",
                DOZ."Zone" AS DOZone,
                T_.tip_amount
                
        FROM public.ny_green_taxi_trips T_
        LEFT JOIN public.ny_trip_zones PUZ
            ON T_."PULocationID" = PUZ."LocationID"
        LEFT JOIN public.ny_trip_zones DOZ
            ON T_."DOLocationID" = DOZ."LocationID"
        WHERE T_.lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
        ) T
    WHERE T.PUZone = 'East Harlem North'
    ORDER BY T.tip_amount DESC
    LIMIT 1
    ;
