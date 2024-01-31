# Load test datastore

Locust is used for performance testing of the datastore.

## Read test
Two tasks are defined: 1) get_data_for_single_timeserie and 2) get_data_single_station_through_bbox. As it is unclear how many users the datastore expect, the test is done for 5 users over 60 seconds in the ci.

### Locust Commands
#### Run locust via web
Example for a single file
```shell
locust -f load-test/locustfile_read.py
```

Example for multiple files
```shell
locust -f load-test/<file_name_1>.py,load-test/<file_name_2>.py
```

#### Run locust only via command line
Example for a single file
```shell
locust -f load-test/locustfile_write.py --headless -u <USERS> -r <SPAWN_RATE> --run-time <RUNTIME> --only-summary --csv store_write
```

Example for multiple locust files
```shell
locust -f load-test/<file_name_1>.py,load-test/<file_name_2>.py --headless -u <USERS> -r <SPAWN_RATE> --run-time <RUNTIME> --only-summary --csv store_write_read
```

## Write test

### Load Estimation
To roughly represent the expected load of the E-SOH system of all EU partners. The setup is 5-min data for 5000 stations, a rate of 17 requests/sec is expected (12*5000/3600).

### Write data using apscheduler
[Advanced Python Scheduler](https://apscheduler.readthedocs.io/en/3.x/) is a package that can be
used to schedule a large amount of jobs.

We represent each station by an apscheduler job, which is scheduled to send data for all variables of that station once every 1, 5 or 10 minutes (randomly chosen). The timestamps in the data correspond to actual clock time, and the data values are randomly chosen. The setup will continue processing data until stopped. This will allow testing of the cleanup functionality of the datastore.

To manually run the data writer, do the following:
```shell
python schedule_write.py
```
