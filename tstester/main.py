#!/usr/bin/env python3
# tested with Python 3.9
# Usage: ./main
import argparse
import json
import pathlib
import random
import sys
from traceback import format_exc

from tstester import TsTester


def parse_args(args):
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Test different data storage solutions for time series of observations.",
        exit_on_error=False,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable logging to stdout.")
    parser.add_argument(
        "-c", "--cfg_file", default="config.json", type=pathlib.Path, help="Config file."
    )
    parser.add_argument("-s", "--random_seed", type=int, default=-1, help="Random seed.")

    pres = parser.parse_args(args)
    return pres.verbose, pres.cfg_file, pres.random_seed


if __name__ == "__main__":
    try:
        verbose, cfg_file, random_seed = parse_args(sys.argv[1:])
        if random_seed >= 0:
            random.seed(random_seed)
        config = json.load(open(cfg_file))
        TsTester(verbose, config).execute()
    except argparse.ArgumentError as e:
        print("failed to parse command-line arguments: {}".format(e), file=sys.stderr)
        sys.exit(1)
    except SystemExit:
        sys.exit(1)  # don't print stack trace in this case (e.g. when --help option is given)
    except Exception:
        sys.stderr.write("error: {}".format(format_exc()))
        sys.exit(1)
