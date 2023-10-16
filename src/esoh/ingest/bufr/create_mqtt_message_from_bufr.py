import sys
import os
from esoh.ingest.bufr.bufresohmsg_py import bufresohmsg_py, \
    init_bufrtables_py, \
    init_oscar_py, \
    destroy_bufrtables_py


def bufr2mqtt(bufr_file_path: str) -> list[str]:
    """
    This function creates the e-soh-message-spec json schema(s) from a BUFR file.

    ### Keyword arguments:
    bufr_file_path (str) -- A BUFR File Path

    Returns:
    str -- mqtt message(s)

    Raises:
    ---
    """

    ret_str = bufresohmsg_py(bufr_file_path)
    return ret_str


if __name__ == "__main__":

    test_path = "../../../../test/test_data/SYNOP_BUFR_2718.bufr"
    msg = ""

    # init_bufrtables_py("/usr/share/eccodes/definitions/bufr/tables/0/wmo/")
    init_bufrtables_py("")
    init_oscar_py("./oscar/oscar_stations_all.json")

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
