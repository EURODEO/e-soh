#!/bin/bash

# Re-Create BUFR dumps files(.txt) and E-SOH (.json)

BUFR_DIR="../../../test/test_data/*buf*"



for f in $BUFR_DIR
do
    echo "Update examples: $f"
    out_file_dir=`dirname $f`
    out_file_name=`basename $f | cut -d"." -f 1`
    TZ=UTC python3 ./bufrprint.py $f > "${out_file_dir}/${out_file_name}.txt"
    TZ=UTC python3 ./create_mqtt_message_from_bufr.py $f > "${out_file_dir}/${out_file_name}.json"
done

