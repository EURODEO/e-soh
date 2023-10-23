import sys
import os
import json
import copy
from esoh.ingest.bufr.bufresohmsg_py import bufresohmsg_py, \
    init_bufrtables_py, \
    init_oscar_py, \
    destroy_bufrtables_py, \
    init_bufr_schema_py


def build_all_json_payloads_from_bufr(bufr_file_path: str) -> list[str]:
    """
    This function creates the e-soh-message-spec json schema(s) from a BUFR file.

    ### Keyword arguments:
    bufr_file_path (str) -- A BUFR File Path

    Returns:
    str -- mqtt message(s)

    Raises:
    ---
    """
    ret_str = []
    msg_str_list = bufresohmsg_py(bufr_file_path)
    for json_str in msg_str_list:
        json_bufr_msg = json.loads(json_str)
        ret_str.append(copy.deepcopy(json_bufr_msg))

    return ret_str


def bufr2mqtt(bufr_file_path: str = "") -> list[str]:
    ret_str = bufresohmsg_py(bufr_file_path)
    return ret_str

if __name__ == "__main__":

    test_path = "./test/test_data/bufr/SYNOP_BUFR_2718.bufr"
    test_schema_path = "./src/esoh/schemas/bufr_to_e_soh_message.json";
    msg = ""

    # init_bufrtables_py("/usr/share/eccodes/definitions/bufr/tables/0/wmo/")
    init_bufrtables_py("")
    init_oscar_py("./src/esoh/ingest/bufr/oscar/oscar_stations_all.json")
    init_bufr_schema_py("./src/esoh/schemas/bufr_to_e_soh_message.json");

    if len(sys.argv) > 1:
        for i, file_name in enumerate(sys.argv):
            if i > 0:
                if os.path.exists(file_name):
                    msg = bufr2mqtt(file_name)
                    for m in msg:
                        print(m)
                else:
                    print("File not exists: {0}".format(file_name))
                    exit(1)

    else:
        msg = bufr2mqtt(test_path)
        for m in msg:
            print(m)

    destroy_bufrtables_py()

    exit(0)
