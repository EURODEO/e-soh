# Database logs
Set UID and GID of the postgres user
`sudo chown -R 101:103 ./logs`


To watch the database logs:

`sudo tail -f ./logs/postgresql-%Y-%m-%d.log`

`sudo tail -f ./logs/postgresql-2024-09-18.log`
