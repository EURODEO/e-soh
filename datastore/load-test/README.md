# Load test datastore

Locust is used for performance testing of the datastore.

## Read test
Two tasks are defined: 1) get_data_for_single_timeserie and 2) get_data_single_station_through_bbox. As it is unclear
how many users the datastore expect, the test is done for 5 users over 60 seconds.

## Write test

### Setup & Data preparation
To resemble the load from all EU partner, we need to multiply KNMI data and increase the input time resolution. For this we
needed to:
* Generate dummy data from the KNMI input data by expanding the 10-minute observations to 5-sec observations.
* Insert the data on a higher temporal resolution

Given that the load test should represent 5-min data for 5000 stations, a rate of 17 requests/sec is needed (5000/(5*60)).
A request contains the observations for all parameters for one station and one timestamp.

Test requirements
* Runtime test = 15min (900s)
* Expected #requests in 15min = 15300 (900*17)
* wait_time between tasks = between 1.5 and 2.5 sec. Resulting in 1 requests per 2 sec per user.
* 35 users should lead to a rate of 17 request/sec, resembling EU coverage.
* User spawn rate = 1 user per sec

### Run locust via web
```text
locust -f load-test/locustfile_write.py
```

### Run locust only via command line
```text
locust -f load-test/locustfile_write.py --headless -u <USERS> -r <SPAWN_RATE> --run-time <RUNTIME> --only-summary --csv store_write
```

### Results
Requests/sec: This is the number of completed requests per second.

|       |      |                |          |                       |                       |
|-------|------|----------------|----------|-----------------------|-----------------------|
| Users | r/s  | Total requests | Failures | Med request time (ms) | Avg request time (ms) |
| 1     | 0.5  | 428            | 0        | 110                   | 110                   |
| 35    | 15.2 | 13 640         | 0        | 180                   | 263                   |
| 55    | 17.2 | 15 439         | 0        | 790                   | 1104                  |



### Read & Write Test
Run the read and write test together to test the load, where the write test will have 7 times more users than the read
test. This is enforced by the weight variable for both user classes.

### Run multiple locust files via web

```text
locust -f load-test/locustfile_write.py,load-test/locustfile_read.py
```

### Run multiple locust files only via command line

```text
locust -f load-test/locustfile_write.py,load-test/locustfile_read.py --headless -u <USERS> -r <SPAWN_RATE> --run-time <RUNTIME> --only-summary --csv store_write_read
```

|       |       |      |                |          |                       |                       |
|-------|-------|------|----------------|----------|-----------------------|-----------------------|
| Test  | Users | r/s  | Total requests | Failures | Med request time (ms) | Avg request time (ms) |
| Write | 35    | 12.7 | 11423          | 0        | 660                   | 696                   |
| Read  | 5     | 69.0 | 62091          | 0        | 20                    | 70                    |
|       |       |      |                |          |                       |                       |
| Write | 53    | 14.5 | 13087          | 0        | 1500                  | 1522                  |
| Read  | 7     | 36.4 | 32769          | 0        | 54                    | 185                   |
