import random


def select_weighted_value(x):
    """Select a random value based on probability weights.

       x is of the form [(v_1, w_1), (v_2, w_2), ..., (v_n, w_n)].
       Returns v_i with a probability of w_i / (w_1 + w_2 + ... + w_n).
    """

    # check preconditions
    if len(x) == 0:
        raise Exception('can\'t select from empty list')
    for item in x:
        if item[1] <= 0:
            raise Exception('non-positive weight not allowed (value: {}, weight: {})'.format(
                item[0], item[1]))

    w_sum_n = sum([z[1] for z in x])  # get total weight sum
    r = random.random() * w_sum_n  # get random value within total weight sum
    w_sum_i = 0  # initialize partial weight sum (w_1 + w_2 + ... + w_i)

    for v, w in x[:-1]:  # loop over items except last one
        w_sum_i += w  # increase partial weight sum
        if r < w_sum_i:
            return v  # inside partial sum, so return value of this item

    return x[-1][0]  # not found, so return value of last item
