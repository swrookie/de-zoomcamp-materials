## Week 3 Homework Solution

SETUP
fhv 2019 data download script

1. Download fhv data

## download.sh

`for i in {2019..2021}
do
    for j in 0{1..9} {10..12}
    do
        wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_$i-$j.csv.gz
    done
done`

2. Upload to GCS

3. Create external table and managed table

`CREATE OR REPLACE EXTERNAL TABLE project-name.nytaxi.external_fhv_tripdata OPTIONS (
  format = 'CSV',
  uris = ['gs://bucket-name/trip data/fhv_tripdata_2019-*.csv.gz']
);`

`CREATE OR REPLACE TABLE project-name.nytaxi.fhv_tripdata_non_partitioned AS
SELECT 
    * 
FROM 
    project-name.nytaxi.external_fhv_tripdata
;`

## Question 1

`SELECT 
    count(*) 
FROM 
    project-name.nytaxi.external_fhv_tripdata
;`

Output:

43244696

Answer: `43,244,696`

## Question 2

`SELECT 
    count(distinct Affiliated_base_number) 
FROM 
    project-name.nytaxi.external_fhv_tripdata
;
`

`
SELECT 
    count(distinct Affiliated_base_number) 
FROM 
    project-name.nytaxi.fhv_tripdata_non_partitioned
;`

Output:

External: 2.52GB (Something wrong with this..)
Managed (BQ): 317.94MB

Answer: 0 MB for the External Table and 317.94MB for the BQ Table

## Question 3

`SELECT 
    count(*) 
FROM 
    project-name.nytaxi.fhv_tripdata_non_partitioned 
WHERE 
    PUlocationID is null and DOlocationID is null
;`

Output: 717748

Answer: 717,748

## Question 4

If table always filter by pickup_datetime(TIMESTAMP) and affiliated_base_number(STRING),
then it would be best to partition by relatively low cardinality TIMESTAMP column (particularly in day granularity)
for efficient partition pruning and then cluster the table by relatively high cardinality affiliated_base_number to scan only the needed rows in partition.

Answer: Partition by pickup_datetime Cluster on affiliated_base_number

## Question 5

`CREATE OR REPLACE TABLE project-name.nytaxi.fhv_tripdata_partitioned_clustered
PARTITION BY
date(pickup_datetime)
CLUSTER BY  
 Affiliated_base_number
AS
SELECT
    *
FROM
  project-name.nytaxi.fhv_tripdata_non_partitioned
  ;
 `

`SELECT
  distinct Affiliated_base_number
FROM
  project-name.nytaxi.fhv_tripdata_non_partitioned
WHERE
  date(pickup_datetime) between '2019-03-01' and '2019-03-31'
;
`

647.87 MB

`SELECT
  distinct Affiliated_base_number
FROM
  project-name.nytaxi.fhv_tripdata_partitioned_clustered
WHERE
  date(pickup_datetime) between '2019-03-01' and '2019-03-31'
;
`

23.05 MB

Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6

Since external table is created with data located in gcs, data is stored in GCS

Answer: GCP Bucket

## Question 7

When clustering table on frequently filtered column, you can prevent unnecessary data being scanned.

Answer: True
