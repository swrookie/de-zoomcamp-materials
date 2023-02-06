## Week 2 Homework Solution

## Question 1

`color = "green"`
`year = 2020`
`month = 1`
`python etl_web_to_gcs.py`

Output:

columns: VendorID float64
lpep_pickup_datetime datetime64[ns]
lpep_dropoff_datetime datetime64[ns]
store_and_fwd_flag object
RatecodeID float64
PULocationID int64
DOLocationID int64
passenger_count float64
trip_distance float64
fare_amount float64
extra float64
mta_tax float64
tip_amount float64
tolls_amount float64
ehail_fee float64
improvement_surcharge float64
total_amount float64
payment_type float64
trip_type float64
congestion_surcharge float64
dtype: object
01:28:08 AM
clean-b9fd7e03-0
rows: 447770

Answer: `447,770`

## Question 2

`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"`
`prefect deployment apply etl_parent_flow-deployment.yaml`
`0 5 1 * *`

Output: At 05:00 AM on day 1 of the month

Answer: 0 5 1 \* \*

## Question 3

`

# print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")

# df["passenger_count"].fillna(0, inplace=True)

# print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

`

`prefect deployment build ./parameterized_el_flow.py:el_parent_flow -n "Parameterized EL"`

`prefect deployment apply el_parent_flow-deployment.yaml`

`prefect agent start  --work-queue "default"`

`SELECT
  count(*)
FROM dtcdecourse.rides
;`

Output: 14851920

Answer: 14,851,920
