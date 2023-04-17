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
PostGISSBE | Keeps all data in PostGIS database with efficient geo search capabilities.
NetCDFSBE_TSMDataInPostGIS | Keeps all data in netCDF files on the local file system, one file per time series (station/param combo). Per time series metadata (i.e. not actual observations) will also be kept in a PostGIS database to speed up the search target files to retrieve observations from.

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

## Environment variables

The following environment variables are supported:

Variable | Mandatory | Default value | Description
:--      | :--       | :--           | :--
`PGHOST`           | No  | `localhost`        | Postgres host
`PGPORT`           | No  | `5432`             | Postgres port number
`PGUSER`           | No  | `postgres`         | Postgres user name
`PGPASSWORD`       | No  | `mysecretpassword` | Postgres password
`PGDBNAME_POSTGIS` | No  | `esoh_postgis`     | Postgres database name for storage backend `PostGISSBE`
`PGDBNAME_NETCDF`  | No  | `esoh_netcdf`      | Postgres database name for storage backend `NetCDFSBE_TSMDataInPostGIS`
`PGOPBACKEND`      | No  | `psycopg2`         | Postgres operation executor backend, one of `psycopg2` or `psql`
`NCDIR`            | No  | `ncdir`            | Directory in which to keep netCDF files

## Configuration file

(TO BE DOCUMENTED - see comments in config.json)

## Running Flake8

Some aspects of the code quality (to follow certain best practices etc.) can be ensured by
running Flake8:

```text
flake8 --ignore=E501,E722 .
```

## Requirements

The program has been tested with:

- Python 3.9
- psycopg2 2.9.3
- psql 15.2 (Ubuntu 15.2-1.pgdg18.04+1)
