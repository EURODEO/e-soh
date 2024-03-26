# E-SOH API

## Enviorment variable
### PROXY_IP

Enviorment variable used to set the `forwarded-allow-ips` in gunicorn. If this API is set behind a proxy, `PROXY_IP` should be set to the proxy IP. Setting this to `*` is possible, but should only be set if you have ensured the API is only reachable from the proxy, and not directly from the internet.