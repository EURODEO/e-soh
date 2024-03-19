#!/usr/bin/env bash

while getopts 'cip' flag; do
  case "${flag}" in
  c) CLIENT=true ;;
  i) INTEGRATION=true ;;
  p) PERFORMANCE=true ;;
  *)
    echo "-c [Run client test]
    -i [Run integration test]
    -p [Run performance test]"
    exit 1
    ;;
  esac
done

# if i is passed run the integration test
if [[ $INTEGRATION ]]; then
  echo "Run Integration test."
  docker compose --env-file ./ci/config/env.list run --rm integration
fi

# if p passed then run performance test
if [[ $PERFORMANCE ]]; then
  cd datastore/load-test || exit 1
  pip install -r requirements.txt

  echo "Run load test (read only)."
  python --version
  python -m grpc_tools.protoc --proto_path=./protobuf datastore.proto --python_out=. --grpc_python_out=.
  locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv store_read

  echo "Run load test (write + read)."
  python -m grpc_tools.protoc --proto_path=./protobuf datastore.proto --python_out=load-test --grpc_python_out=load-test
  python schedule_write.py > schedule_write.log 2>&1 &
  locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv store_rw
  kill %1
  echo Catting schedule_write output...
  cat schedule_write.log
  echo Done catting
  cd ../.. || exit 1
fi

# if c passed then run client
if [[ $CLIENT ]]; then
  echo "Run Client test."
  docker compose --env-file ./ci/config/env.list run --rm client
fi
