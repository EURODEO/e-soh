# Load test datastore


## Read test
Locust is used for read performance testing of the datastore.

Two tasks are defined: 1) `get_data_for_single_timeserie` and 2) `get_data_single_station_through_bbox`.
Each user does one task, as soon as the query completes, it does another.
We found that the maximum total throughput is reached with about 5 users.

### Locust Commands
#### Run locust via web
```shell
locust -f load-test/locustfile_read.py
```

#### Run locust only via command line
```shell
locust -f load-test/locustfile_read.py --headless -u <USERS> -r <SPAWN_RATE> --run-time <RUNTIME> --only-summary --csv store_write
```

## Write test

### Load Estimation
The expected load of the E-SOH system is data every 5 minutes for 5000 stations,
which gives a rate of 17 requests/sec (12*5000/3600).

### Write data using apscheduler
[Advanced Python Scheduler](https://apscheduler.readthedocs.io/en/3.x/) is a package that can be
used to schedule a large amount of jobs.

We represent each station by an apscheduler job,
which is scheduled to send data for all variables of that station once every 1, 5 or 10 minutes (randomly chosen).
This roughly represents the expected load of the E-SOH system of all EU partners.
The timestamps in the data correspond to actual clock time, and the data values are randomly chosen.
The setup will continue processing data until stopped.
This will allow testing of the cleanup functionality of the datastore.

To manually run the data writer, do the following:
```shell
python schedule_write.py
```
