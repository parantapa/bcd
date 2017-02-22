# encoding: utf-8
# pylint: disable=redefined-outer-name
"""
Test the dset module.
"""

import random
import pytest

import pbdset as ds

@pytest.fixture(params=["none", "zlib", "lz4", "xz", "zstd"])
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

    fname = tmpdir.join("test.dset").strpath

    params = {
        "block_length": block_length,
        "comp_format": comp_format,
        "comp_level": comp_level,
    }

    with ds.open(fname, "w", **params) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        ret_data = list(dset)
        assert test_data == ret_data

def test_append(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        for item in test_data:
            dset.append(item)

    with ds.open(fname) as dset:
        ret_data = list(dset)
        assert test_data == ret_data

def test_extend(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        ret_data = list(dset)
        assert test_data == ret_data

def test_append_mode_2parts(tmpdir, test_data, block_length):
    """
    Test the append mode usage.
    """

    random.seed(42)
    for x in xrange(10):
        fname = tmpdir.join("test-iter-%d.dset" % x).strpath
        n = random.randint(0, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n]
        part2 = test_data[n:]

        # Insert into dset separately
        with ds.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with ds.open(fname, "a") as dset:
            dset.extend(part2)

        with ds.open(fname) as dset:
            ret_data = list(dset)
            assert test_data == ret_data

def test_append_mode_3parts(tmpdir, test_data, block_length):
    """
    Test the append mode usage.
    """

    random.seed(42)
    for x in xrange(10):
        fname = tmpdir.join("test-iter-%d" % x).strpath
        n1 = random.randint(0, len(test_data) - 1)
        n2 = random.randint(n1, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n1]
        part2 = test_data[n1:n2]
        part3 = test_data[n2:]

        # Insert into dset separately
        with ds.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with ds.open(fname, "a") as dset:
            dset.extend(part2)

        # Second time open in append mode
        with ds.open(fname, "a") as dset:
            dset.extend(part3)

        with ds.open(fname) as dset:
            ret_data = list(dset)
            assert test_data == ret_data

def test_empty_read_raises(tmpdir):
    """
    Test raise on empty file raises.
    """

    fname = tmpdir.join("test.dset").strpath

    with pytest.raises(IOError):
        with ds.open(fname):
            pass

    with pytest.raises(IOError):
        with ds.open(fname, "a"):
            pass

    with ds.open(fname, "w", 1):
        pass
    with pytest.raises(IOError):
        with ds.open(fname, "w", 1):
            pass

def test_append_mode_crash(tmpdir, test_data, block_length):
    """
    Simulate crash while appending.
    """

    random.seed(42)
    for x in xrange(10):
        fname = tmpdir.join("test-iter-%d" % x).strpath
        n = random.randint(0, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n]
        part2 = test_data[n:]

        # Insert into dset separately
        with ds.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with ds.open(fname, "a") as dset:
            dset.extend(part2)

            # This should stop the close function
            # from doing its job
            dset.closed = True
            dset.store.closed = True
            dset.store.env.close()

        # We should get back the first part intact
        with ds.open(fname) as dset:
            ret_data = list(dset)
            ret_data = ret_data[:len(part1)]
            assert part1 == ret_data

def test_get_idx(tmpdir, test_data, block_length):
    """
    Test the get_idx version.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        for i in xrange(len(test_data)):
            assert test_data[i] == dset.get_idx(i)
            assert test_data[i] == dset[i]

            assert test_data[-i] == dset.get_idx(-i)
            assert test_data[-i] == dset[-i]

def test_get_idxs(tmpdir, test_data, block_length):
    """
    Test the multiple index version.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Do ten random samples
        for _ in xrange(10):
            idxs = random.sample(all_idxs, len(all_idxs) // 10)
            idxs = sorted(idxs)

            vals = dset.get_idxs(idxs)
            for i, v in zip(idxs, vals):
                assert test_data[i] == v
            vals = dset[idxs]
            for i, v in zip(idxs, vals):
                assert test_data[i] == v

def test_get_slice(tmpdir, test_data, block_length):
    """
    Test slice version.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Test the heads and tails
        for _ in xrange(10):
            n = random.choice(all_idxs)

            assert test_data[:n] == list(dset.get_slice(None, n))
            assert test_data[:n] == dset[:n]

            assert test_data[n:] == list(dset.get_slice(n, None))
            assert test_data[n:] == dset[n:]

def test_get_slice_wstep(tmpdir, test_data, block_length):
    """
    Test slice version with steps.
    """

    fname = tmpdir.join("test.dset").strpath

    with ds.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with ds.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Do 100 times
        for _ in xrange(100):
            start, stop = sorted(random.sample(all_idxs, 2))

            # With half probability use negative step
            if random.random() < 0.5:
                step = -1

            # With half probability flip start, stop
            if random.random() < 0.5:
                stop, start = start, stop

            # With half probability do negative indexes
            if random.random() < 0.5:
                stop = stop - len(test_data)
                start = start - len(test_data)

            stop = stop + step

            assert test_data[start:stop:step] == list(dset.get_slice(start, stop, step))
            assert test_data[start:stop:step] == dset[start:stop:step]
