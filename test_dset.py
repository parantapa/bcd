# encoding: utf-8
# pylint: disable=redefined-outer-name
"""
Test the dset module.
"""

import pytest

import dset

@pytest.fixture(params=["none", "zlib", "lz4", "xz"])
def comp_format(request):
    return request.param

@pytest.fixture(params=[1, 6, 9])
def comp_level(request):
    return request.param

@pytest.fixture(params=[10, 100, 7, 97])
def block_length(request):
    return request.param

@pytest.fixture(params=[1000, 1001, 10000, 10001])
def test_data(request):
    return range(request.param)

def test_simple(tmpdir, test_data, block_length, comp_format, comp_level):
    """
    Simple dset test.
    """

    fname = "test-%s-%d.dset" % (comp_format, comp_level)
    fname = tmpdir.join(fname).strpath

    params = {
        "block_length": block_length,
        "comp_format": comp_format,
        "comp_level": comp_level,
    }

    with dset.open(fname, "w", **params) as ds:
        for x in test_data:
            ds.append(x)

    with dset.open(fname) as ds:
        ret_data = [ds.get_idx(i) for i in xrange(len(ds))]
        assert test_data == ret_data
