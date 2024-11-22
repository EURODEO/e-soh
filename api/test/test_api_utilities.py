import pytest

from utilities import get_levels_values

levels_in_out = [
    ("1", ["100"]),
    ("1,2", ["100", "200"]),
    ("1,2, 3", ["100", "200", "300"]),
    ("1/3", ["100/300"]),
    ("../3", ["../300"]),
    ("1/..", ["100/.."]),
    ("R3/1.2/0.3", ["120", "150", "180"]),
    ("1, 3/5, R3/5/0.1,11", ["100", "300/500", "500", "510", "520", "1100"]),
]


@pytest.mark.parametrize("levels_in, levels_out", levels_in_out)
def test_get_levels_values(levels_in, levels_out):
    assert get_levels_values(levels_in) == levels_out
