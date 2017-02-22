# encoding: utf-8
# pylint: disable=too-many-instance-attributes
# pylint: disable=attribute-defined-outside-init
#
#            ____________________/\
#            \______   \______   )/______
#             |     ___/|    |  _//  ___/
#             |    |    |    |   \\___ \
#             |____|    |______  /____  >
#                              \/     \/
# ________          __                        __
# \______ \ _____ _/  |______    ______ _____/  |_
#  |    |  \\__  \\   __\__  \  /  ___// __ \   __\
#  |    `   \/ __ \|  |  / __ \_\___ \\  ___/|  |
# /_______  (____  /__| (____  /____  >\___  >__|
#         \/     \/          \/     \/     \/
"""
Read and write PB's Dataset files.
"""

import sys
import os.path

import struct
import lmdb

# Import comression/decompression functions
from zlib import compress as zlib_comp, decompress as zlib_decomp
from lz4 import compress as _lz4_comp, decompress as lz4_decomp
from backports.lzma import compress as _xz_comp, \
                           decompress as _xz_decomp, \
                           CHECK_NONE

def lz4_comp(data, _):
    return _lz4_comp(data)

def xz_comp(data, level):
    return _xz_comp(data, preset=level, check=CHECK_NONE)

def xz_decomp(data):
    return _xz_decomp(data)

# We serialize using msgpack
from msgpack import packb as _packb, unpackb as _unpackb

def pack(x):
    return _packb(x, use_bin_type=True)

def unpack(x, default=None):
    if x is None:
        return default
    else:
        return _unpackb(x, encoding="utf-8")

# Setup the checksum function
from zlib import adler32

def checksum(data):
    return adler32(data) & 0xffffffff

COMP_TABLE = {
    "none": lambda data, level: data,
    "zlib": zlib_comp,
    "lz4": lz4_comp,
    "xz": xz_comp,
}

DECOMP_TABLE = {
    "none": lambda comp: comp,
    "zlib": zlib_decomp,
    "lz4": lz4_decomp,
    "xz": xz_decomp
}

VERSION = 0.1

class Closes(object): # pylint: disable=too-few-public-methods
    """
    Runs close() on context exiting and garbage collection.
    """

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def close(self):
        raise NotImplementedError()

class DataStore(Closes):
    """
    An abstraction layer over underlying data store.
    """

    META_KEYS = frozenset([
        "version", "block_length", "length", "comp_format", "comp_level"
    ])

    DEFAULT_PARAMS = {
        "map_size": 2 ** 40,
        "subdir": False,
        "readonly": True,
        "metasync": False,
        "sync": False,
        "mode": 0o644,
        "readahead": False,
        "writemap": True,
        "meminit": False,
        "map_async": True,
        "max_dbs": 2,
        "lock": False,
    }

    def __init__(self, fname, write=False, create=False):
        self.closed = True

        self.fname = fname
        self.write = write
        self.create = create

        _exists = os.path.exists(fname)
        if create and _exists:
            raise IOError("File '%s' already exists" % fname)
        if not create and not _exists:
            raise IOError("File '%s' doesn't exist" % fname)

        params = dict(DataStore.DEFAULT_PARAMS)
        params["readonly"] = not write
        self.env = lmdb.open(self.fname, **params)
        try:
            self.meta_db = self.env.open_db("meta", create=create)
            self.block_db = self.env.open_db("block", create=create)

            self.txn = self.env.begin(write=write, buffers=True)

            self.closed = False
        except Exception:
            self.env.close()
            raise

    def close(self):
        if not self.closed:
            self.txn.commit()
            if self.write:
                self.env.sync(True)
            self.env.close()
            self.closed = True

    def get(self, i):
        ib = struct.pack(">I", i)
        return self.txn.get(ib, db=self.block_db)

    def put(self, i, block):
        ib = struct.pack(">I", i)
        self.txn.put(ib, block, db=self.block_db)

    def __getattr__(self, key):
        if key not in DataStore.META_KEYS:
            raise AttributeError("Unknown attribute: '%s'" % key)

        value = self.txn.get(key, db=self.meta_db)
        if value is None:
            return None
        else:
            return unpack(value)

    def __setattr__(self, key, value):
        if key in DataStore.META_KEYS:
            self.txn.put(key, pack(value), db=self.meta_db)
        else:
            self.__dict__[key] = value

def comp_block(block_raw, comp_fn, comp_level):
    """
    Compress the block and add header.
    """

    block_chksum = checksum(block_raw)
    block_comp = comp_fn(block_raw, comp_level)
    header = struct.pack("<II", len(block_raw), block_chksum)
    block_hcomp = header + block_comp

    return block_hcomp

def decomp_block(block_hcomp, decomp_fn):
    """
    Decompress the block.
    """

    len_block_raw, stored_chksum = struct.unpack_from("<II", block_hcomp)
    block_comp = buffer(block_hcomp, 8, len(block_hcomp) - 8)
    block_raw = decomp_fn(block_comp)
    block_chksum = checksum(block_raw)

    if len(block_raw) != len_block_raw:
        raise IOError("Size mismatch: %d != %d"
                      % (len(block_raw), len_block_raw))
    if block_chksum != stored_chksum:
        raise IOError("Checksum mismatch: %0x != %0x"
                      % (block_chksum, stored_chksum))

    return block_raw

class DatasetReader(Closes):
    """
    Read entries from a dataset file.
    """

    def __init__(self, fname):
        self.closed = True

        self.store = DataStore(fname)
        try:
            if self.store.version != VERSION:
                raise IOError("Invalid version: %d" % self.store.version)

            self.block_length = self.store.block_length
            self.length = self.store.length

            self.comp_format = self.store.comp_format
            self.comp_level = self.store.comp_level

            try:
                self.decomp_fn = DECOMP_TABLE[self.comp_format]
            except KeyError:
                raise IOError("Unknown compression: %s" % self.comp_format)

            self.closed = False
        except Exception:
            self.store.close()
            raise

        # number of blocks already present in the dataset
        self.num_blocks  = self.length // self.block_length
        self.num_blocks += bool(self.length % self.block_length)

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    def close(self):
        if not self.closed:
            self.store.close()
            self.closed = True

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        block_hcomp = self.store.get(i)
        if block_hcomp is None:
            raise IOError("Block %d not in store" % i)
        try:
            block_raw = decomp_block(block_hcomp, self.decomp_fn)
        except IOError as e:
            raise IOError("Block %d: %s", (i, e)), None, sys.exc_info()[2]
        return unpack(block_raw)

    def __len__(self):
        return self.length

    def get_idx(self, n):
        """
        Get the value at given idx.
        """

        n = (self.length + n) if n < 0 else n
        if n < 0 or n >= self.length:
            raise IndexError("Index out of range")

        i = n // self.block_length
        j = n % self.block_length
        if self.cur_block_idx != i:
            self.cur_block = self.load_block(i)
            self.cur_block_idx = i
        return unpack(self.cur_block[j])

    def get_slice(self, *args):
        """
        Return iterable for the given range.
        """

        _block_length = self.block_length

        start, stop, step = slice(*args).indices(self.length)

        # Find the number of items in slice
        n = (stop - start) // step
        if n <= 0:
            return

        # Check if begin and end indexes are in range
        if start < 0 or start >= self.length:
            raise IndexError("Index out of range")
        end = start + (n - 1) * step
        if end < 0 or end >= self.length:
            raise IndexError("Index out of range")

        # Do the actual loop
        # This doesn't use the class's cur_block
        cur_block_idx = -1
        cur_block = None
        for n in xrange(start, stop, step):
            i = n // _block_length
            j = n % _block_length
            if cur_block_idx != i:
                cur_block = self.load_block(i)
                cur_block_idx = i
            yield unpack(cur_block[j])

    def get_idxs(self, ns):
        """
        Get the values at given idxs.

        NOTE: if the indexes are not sorted,
        performance may be really slow.
        """

        _block_length = self.block_length

        cur_block_idx = -1
        cur_block = None
        for n in ns:
            n = (self.length + n) if n < 0 else n
            if n < 0 or n >= self.length:
                raise IndexError("Index out of range")

            i = n // _block_length
            j = n % _block_length
            if cur_block_idx != i:
                cur_block = self.load_block(i)
                cur_block_idx = i
            yield unpack(cur_block[j])

    def __iter__(self):
        for i in xrange(self.num_blocks):
            cur_block = self.load_block(i)
            for item in cur_block:
                yield unpack(item)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return list(self.get_slice(key.start, key.stop, key.step))
        elif isinstance(key, (list, tuple)):
            return list(self.get_idxs(key))
        else:
            return self.get_idx(key)

class DatasetWriter(Closes):
    """
    Writes a dataset object to a file.
    """

    def __init__(self, fname, create=True, block_length=1,
                 comp_format="lz4", comp_level=6):
        self.closed = True

        # Check the parameters
        block_length = int(block_length)
        if block_length < 1:
            raise ValueError("Block length must be at-least 1")
        if comp_format not in DECOMP_TABLE:
            raise IOError("Unknown compression: %s" % comp_format)
        comp_level = int(comp_level)
        if not 1 <= comp_level <= 9:
            raise ValueError("Invalid compression level: %d" % comp_level)

        self.fname = fname

        self.store = DataStore(fname, write=True, create=create)
        try:
            if create:
                self.block_length = block_length
                self.length = 0

                self.comp_format = comp_format
                self.comp_level = comp_level

                self.write_meta(True)
            else:
                if self.store.version != VERSION:
                    raise IOError("Invalid version: %d" % self.store.version)

                self.block_length = self.store.block_length
                self.length = self.store.length

                self.comp_format = self.store.comp_format
                self.comp_level = self.store.comp_level

            self.comp_fn = COMP_TABLE[self.comp_format]
            self.decomp_fn = DECOMP_TABLE[self.comp_format]

            self.closed = False
        except:
            self.store.close()
            raise

        # number of blocks already present in the dataset
        self.num_blocks  = self.length // self.block_length
        self.num_blocks += bool(self.length % self.block_length)

        if self.length % self.block_length == 0:
            self.cur_block = []
        else:
            self.cur_block = self.load_block(self.num_blocks -1)
            self.num_blocks -= 1

    def write_meta(self, full=False):
        """
        Write meta information.
        """

        if full:
            self.store.version = VERSION
            self.store.block_length = self.block_length
            self.store.comp_format = self.comp_format
            self.store.comp_level = self.comp_level

        self.store.length = self.length

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        block_hcomp = self.store.get(i)
        if block_hcomp is None:
            raise IOError("Block %d not in store" % i)
        try:
            block_raw = decomp_block(block_hcomp, self.decomp_fn)
        except IOError as e:
            raise IOError("Block %d: %s", (i, e)), None, sys.exc_info()[2]
        return unpack(block_raw)

    def dump_block(self, i, block):
        """
        Write the block to the store.
        """

        block_raw = pack(block)
        block_hcomp = comp_block(block_raw, self.comp_fn, self.comp_level)
        self.store.put(i, block_hcomp)
        self.write_meta()

    def flush(self, force=False):
        """
        Flush the current block to output file.
        """

        if len(self.cur_block) != self.block_length and not force:
            raise ValueError("Cant flush unfilled block without forcing")

        if not self.cur_block:
            return

        self.dump_block(self.num_blocks, self.cur_block)

        self.num_blocks += 1
        self.cur_block = []

    def close(self):
        if not self.closed:
            self.flush(force=True)
            self.store.close()
            self.closed = True

    def append(self, obj):
        """
        Append the object to database.
        """

        self.cur_block.append(pack(obj))
        self.length += 1

        if len(self.cur_block) == self.block_length:
            self.flush()

    def extend(self, iterable):
        for item in iterable:
            self.cur_block.append(pack(item))
            self.length += 1

            if len(self.cur_block) == self.block_length:
                self.flush()

def open(fname, mode="r", block_length=None, comp_format="lz4", comp_level=6):
    # pylint: disable=redefined-builtin
    """
    Open a dataset for reading or writing.
    """

    if mode == "r":
        return DatasetReader(fname)
    elif mode == "w":
        if block_length is None:
            raise ValueError("Must specify block_length for write mode")
        return DatasetWriter(fname, True, block_length, comp_format, comp_level)
    elif mode == "a":
        return DatasetWriter(fname, False)
    else:
        raise ValueError("Invalid mode '%s'" % mode)
