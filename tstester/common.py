import os
import random
import subprocess
import time


def select_weighted_value(x):
    """Select a random value based on probability weights.

    x is of the form [(v_1, w_1), (v_2, w_2), ..., (v_n, w_n)].
    Returns v_i with a probability of w_i / (w_1 + w_2 + ... + w_n).
    """

    # check preconditions
    if len(x) == 0:
        raise Exception("can't select from empty list")
    for item in x:
        if item[1] <= 0:
            raise Exception(
                "non-positive weight not allowed (value: {}, weight: {})".format(item[0], item[1])
            )

    w_sum_n = sum([z[1] for z in x])  # get total weight sum
    r = random.random() * w_sum_n  # get random value within total weight sum
    w_sum_i = 0  # initialize partial weight sum (w_1 + w_2 + ... + w_i)

    for v, w in x[:-1]:  # loop over items except last one
        w_sum_i += w  # increase partial weight sum
        if r < w_sum_i:
            return v  # inside partial sum, so return value of this item

    return x[-1][0]  # not found, so return value of last item


def now_secs():
    """Return the current time in seconds since the Epoch.
    See help(time.time).
    """
    return time.time()


def elapsed_secs(start_secs):
    """Return the elapsed secs since start_secs."""
    return now_secs() - start_secs


def get_env_var(name, default_value="", fail_on_empty=True):
    """Get environment variable."""
    v = os.getenv(name, default_value)
    if (v == "") and fail_on_empty:
        raise Exception("environment variable {} empty or undefined".format(name))
    return v


def exec_command(cmd):
    """Execute a command, returning stdout on success, raising an error on failure."""
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        raise Exception(
            "\n'{}' failed:\n  EXIT CODE: {}\n  STDOUT: '{}'\n  STDERR: '{}'\n".format(
                cmd, p.returncode, p.stdout.strip(), p.stderr.strip()
            )
        )
    return p.stdout


def ts_merge(t1, v1, t2, v2, oldest_time=None):
    """Merges observation time series t2/v2 into t1/v1

    - t1 and t2 are lists of UNIX timestamp seconds to represent obs times
    - v1 and v2 are lists floats to represent obs values
    - if oldest_time is not None, it is a UNIX timestamp indicating the oldest allowable obs time

    Precondition:
        for j in [1,2]:
            len(tj) == len(vj)
            len(tj) > 0 => tj[-1] < 2**31-1
            len(tj) > 1 => for all 1 < i < len(tj): tj[i-1] < tj[i]
    (i.e.: t1/v1 and t2/v2 are two time series, possibly empty, with strictly monotonically
    increasing obs times that are all earlier than 2**31-1)

    The function merges time series 1 and 2 into a new time series that contains the observations
    from 1 and 2 sorted on obs time in the same way. Observations at the same time will result in a
    single observation with the value from v2.

    Returns merged time series as two lists for times and values respectively.
    """

    sentinel_obs_time = 2**31 - 1  # assuming no obs time is this high (part of precondition!)

    def validate_precondition():
        t = (t1, t2)
        v = (v1, v2)
        for j in [0, 1]:
            if len(t[j]) != len(v[j]):
                raise Exception(
                    "precondition failed: len(t[{}]) ({}) != len(v[{}]) ({})".format(
                        j, len(t[j]), j, len(v[j])
                    )
                )
            if len(t[j]) > 0:
                if t[j][-1] >= sentinel_obs_time:
                    raise Exception(
                        "precondition failed: t[{}][-1] >= {}".format(j, sentinel_obs_time)
                    )
            if len(t[j]) > 1:
                for i in range(1, len(t[j])):
                    if t[j][i - 1] >= t[j][i]:
                        raise Exception(
                            "precondition failed: t[{}][{}] ({}) >= t[{}][{}] ({})".format(
                                j, i - 1, t[j][i - 1], j, i, t[j][i]
                            )
                        )

    validate_precondition()

    def remove_old_observations():
        t = [t1, t2]
        v = [v1, v2]
        for j in [0, 1]:
            pos = next((i for i, v in enumerate(t[j]) if v >= oldest_time), -1)  # find pos of
            # first obs that is not too old
            if pos > 0:  # remove first part of time series since it is too old
                t[j], v[j] = t[j][pos:], v[j][pos:]
            elif pos == -1:  # entire time series too old, so empty it completely
                t[j], v[j] = [], []
        return t[0], v[0], t[1], v[1]

    if oldest_time is not None:
        t1, v1, t2, v2 = remove_old_observations()

    # add sentinel obs time (to save explicit if tests in below algorithm)
    t1.append(sentinel_obs_time)
    t2.append(sentinel_obs_time)

    i, j, t3, v3 = 0, 0, [], []

    # Have time series 1 and 2 aggregate sub-series observations in an alternating way to maintain
    # order of final time series. Each of the internal loops increments its index until the other
    # time series is overtaken or an identical obs time is found. Eventually, both loops will stop
    # at the sentinel obs time. Aggregating within the internal loops corresponds to
    # 'INSERT without conflict' in SQL.
    while True:
        while t1[i] < t2[j]:
            t3.append(t1[i])
            v3.append(v1[i])
            i += 1

        while t2[j] < t1[i]:
            t3.append(t2[j])
            v3.append(v2[j])
            j += 1

        if t1[i] == t2[j]:  # identical obs time
            if t1[i] == sentinel_obs_time:
                break  # reached end of both time series, so we're done!
            # aggregate into final result by prioritizing obs value in t2 (corresponding to
            # 'INSERT with conflict' in SQL)
            t3.append(t2[j])
            v3.append(v2[j])
            i += 1
            j += 1

        # not done, so iterate once more

    return t3, v3
