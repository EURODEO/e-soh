# E-SOH API

## Enviorment variable
### FORWARDED_ALLOW_IPS

Environment variable used to set the `forwarded-allow-ips` in gunicorn. If this API is set behind a proxy, `FORWARDED_ALLOW_IPS` should be set to the proxy IP. Setting this to `*` is possible, but should only be set if you have ensured the API is only reachable from the proxy, and not directly from the internet. If not using docker compose this have to be passed to docker using the `-e` argument.
