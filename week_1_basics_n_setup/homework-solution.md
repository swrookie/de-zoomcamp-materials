## Week 1 Homework Solution

## Question 1

`sudo docker build --help | grep "Write the image ID to the file"`

Output: --iidfile string Write the image ID to the file

Answer: `--iidfile string`

## Question 2

`sudo docker run -it --entrypoint bash python:3.9`
`pip list`

Output: pip, setuptools, wheel

Answer: 3

## Question 3

`SELECT
	count(*)
FROM
	green_taxi_trips
WHERE
	lpep_pickup_datetime >= '2019-01-15 00:00:00' and lpep_dropoff_datetime < '2019-01-16 00:00:00'
;`

Answer: 20530

## Question 4

`SELECT
    cast(lpep_pickup_datetime as date) pickup_date
FROM
    green_taxi_trips
ORDER BY
    trip_distance desc
LIMIT 1
;`

Answer: 2019-01-15

## Question 5

`SELECT
	count(case when passenger_count = 2 then 1 else null end) two_passenger_trips
	, count(case when passenger_count = 3 then 1 else null end) three_passenger_trips
FROM
	green_taxi_trips
WHERE
	lpep_pickup_datetime >= '2019-01-01 00:00:00' AND lpep_pickup_datetime < '2019-01-02 00:00:00'
;`

Answer: 2: 1282 ; 3: 254

## Question 6

`SELECT
	dz."Zone" drop_off_zone
FROM
	green_taxi_trips gtt
	INNER JOIN zones pz ON gtt."PULocationID" = pz."LocationID"
	INNER JOIN zones dz ON gtt."DOLocationID" = dz."LocationID"
WHERE
	pz."Zone" = 'Astoria'
ORDER BY
	gtt.tip_amount desc
LIMIT 1
;`

Answer: Long Island City/Queens Plaza
