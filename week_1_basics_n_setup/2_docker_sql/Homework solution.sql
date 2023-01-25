# Q1
docker build --help
--iidfile string          Write the image ID to the file

# Q2

docker run -it --entrypoint bash python:3.9
pip list

--output
package  |  version
--------  --------
pip         22.0.4
setuptools  58.1.0
wheel       0.38.4

# Q3 

select COUNT(1)
from green_taxi_trips
where (lpep_pickup_datetime > '2019-01-15 00:00:00'
	and lpep_pickup_datetime < '2019-01-16 00:00:00')
	and  (lpep_dropoff_datetime  > '2019-01-15 00:00:00'
	and lpep_dropoff_datetime  < '2019-01-16 00:00:00')

output : 20,530

# Q4

select  CAST(lpep_pickup_datetime AS date) AS pu_date , max(trip_distance)   as max_trip_distance
from green_taxi_trips
group by CAST(lpep_pickup_datetime AS date), trip_distance  
order by trip_distance DESC;

# Q5

select  distinct passenger_count, count(passenger_count) as cnt  
from green_taxi_trips
where CAST(lpep_pickup_datetime AS date) = '2019-01-01'
and (passenger_count in (2,3))
group by passenger_count ;

--output 2: 1282, 3:254

# Q6
select gt."DOLocationID" , tz."Zone", MAX(gt.tip_amount)
	from green_taxi_trips gt
		inner join taxi_zone_lkp tz on gt."DOLocationID"  = tz."LocationID"  
	where gt."PULocationID" =7
	group by 1, 2
	order by max desc;

-- answer: Long Island City/Queens Plaza	88.0
