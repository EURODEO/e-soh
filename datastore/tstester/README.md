# TsTester

(**NOTE:** this code is still under development!)

## Overview

This directory contains a Python program to test and compare the performance of different backends
for storing the most recent (typically latest 24H) observations of a set of time series.
(Note: The storage is intended as a dynamic buffer to provide fast access to commonly used
operations. A longer-term representation of the time series may thus be kept elsewhere.)

Two backends are currently implemented (the code is designed to make it easy to add more):

Name | Description
:--  | :--
TimescaleDBSBE | Keeps all data in a TimescaleDB database extended with PostGIS.
PostGISSBE | Keeps all data in a Postgres database extended with PostGIS.
NetCDFSBE_TSMDataInPostGIS | Keeps all data in netCDF files on the local file system, one file per time series (station/param combo). Per time series metadata (i.e. not actual observations) will also be kept in a Postgres database extended with PostGIS to speed up searching for target files to retrieve observations from.

Assumptions:

- a time series is essentially a sequence of (obs time, obs value) tuples for a specific
  (station, parameter) combo

- a station represents the sensor at a fixed lat,lon location

- a parameter is a scalar (floating point value) that represents an observable quantity,
  like air temperature

## Executing the program

The program is executed like this:

```text
./main.py
```

or

```text
python main.py
```

Tip: use the -h option to get help on command-line arguments:

```text
./main.py -h
```

## Output

Test results (such as timing stats) are written in JSON format to standard output:

```text
... ./main.py ... > stats.json
```

## Environment variables

The following environment variables are supported:

Variable | Mandatory | Default value | Description
:--      | :--       | :--           | :--
`TSDBHOST`         | No  | `localhost`        | TimescaleDB host
`TSDBPORT`         | No  | `5433`             | TimescaleDB port number
`TSDBUSER`         | No  | `postgres`         | TimescaleDB user name
`TSDBPASSWORD`     | No  | `mysecretpassword` | TimescaleDB password
`TSDBDBNAME`       | No  | `esoh`             | TimescaleDB database name for storage backend `TimescaleDBSBE`
`PGHOST`           | No  | `localhost`        | Postgres host
`PGPORT`           | No  | `5432`             | Postgres port number
`PGUSER`           | No  | `postgres`         | Postgres user name
`PGPASSWORD`       | No  | `mysecretpassword` | Postgres password
`PGDBNAME_POSTGIS` | No  | `esoh_postgis`     | Postgres database name for storage backend `PostGISSBE`
`PGDBNAME_NETCDF`  | No  | `esoh_netcdf`      | Postgres database name for storage backend `NetCDFSBE_TSMDataInPostGIS`
`PGOPBACKEND`      | No  | `psycopg2`         | Postgres operation executor backend, one of `psycopg2` or `psql` (**WARNING:** the connection time for `psycopg2` can be quite long in some environments, sometimes up to 15-20 secs! On the other hand, once connected, `psycopg2` performs faster than `psql`, so `psycopg2` should be used for most cases in practice)
`NCDIR`            | No  | `ncdir`            | Directory in which to keep netCDF files

## Configuration file

(TO BE DOCUMENTED - see comments in config.json)

## Running Flake8

Some aspects of the code quality (to follow certain best practices etc.) can be ensured by
running Flake8:

```text
flake8 --ignore=E501,E722 .
```

## Software versions

The program has been run successfully with the following software versions:

- Python 3.9
- psycopg2 2.9.3
- psql 15.2 (Ubuntu 15.2-1.pgdg18.04+1)

## Running PostGIS in docker container on local machine

The program can run against any PostGIS instance, for example one running in a local docker
container set up like this:

```text
$ docker --version
Docker version 23.0.3, build 3e7cbfd
$ docker pull postgis/postgis:15-3.3
...
$ docker images -a
REPOSITORY        TAG       IMAGE ID       CREATED         SIZE
postgis/postgis   15-3.3    b4f38bb1dc5d   7 days ago      588MB
$ docker run --name some-postgis -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgis/postgis
...
$ docker ps -a
CONTAINER ID   IMAGE             COMMAND                  CREATED       STATUS                  PORTS                                       NAMES
21b793e18be7   postgis/postgis   "docker-entrypoint.s…"   2 hours ago   Up 2 hours              0.0.0.0:5432->5432/tcp, :::5432->5432/tcp   some-postgis
...
```

## Running TimescaleDB in docker container on local machine

TODO!
