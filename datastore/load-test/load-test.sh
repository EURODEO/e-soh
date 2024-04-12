#!/usr/bin/env bash

echo "Run load test (read only)."; \
  locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv "${DOCKER_PATH}/output/store_read"; \
  echo "Run load test (write + read)."; \
  python schedule_write.py > "${DOCKER_PATH}/output/schedule_write.log" 2>&1 & \
      locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv "${DOCKER_PATH}/output/store_rw" && \
      kill %1 && \
      echo "Catting schedule_write output..." && \
      cat "${DOCKER_PATH}/output/schedule_write.log" && \
      echo "Done catting"
