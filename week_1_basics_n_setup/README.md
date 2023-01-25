# Running postGres container
docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-p 5432:5432 \
	--network=pg-network \
	--name pgdatabase \
	postgres:13

# Running pgAdmin in a docker container
docker run --rm \
    -p 5050:5050 \
    --network=pg-network \
    --name pgadmin \
    thajeztah/pgadmin4

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

URL="http://192.168.1.8:8000/taxi+_zone_lookup.csv"

python ingest_data.py \
	--user=root \
	--password=root \
	--host=localhost \
	--port=5432 \
	--db=ny_taxi \
	--table_name=taxi_zone_lkp \
	--url="${URL}"

docker build -t taxi_ingest:v001 .

URL="http://192.168.1.8:8000/yellow_tripdata_2021-01.csv"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="${URL}"


docker run -it --entrypoint bash python:3.9




