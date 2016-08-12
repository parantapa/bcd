# encoding: utf-8
# pylint: disable=too-many-instance-attributes
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

import struct
import lmdb

# Import comression/decompression functions
from zlib import compress as zlib_comp, decompress as zlib_decomp
from lz4 import compress as _lz4_comp, decompress as lz4_decomp
from backports.lzma import compress as _xz_comp, decompress as _xz_decomp, CHECK_NONE

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

def unpack(x):
    return _unpackb(x, encoding="utf-8")

def unpack_default(x, default):
    if x is None:
        return default
    else:
        return unpack(x)

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

DEFAULT_PARAMS = {
    "map_size": 2 ** 40,
    "subdir": False,
    "readonly": True,
    "lock": True,
    "max_dbs": 3
}

def idx_bin(i):
    return struct.pack("> I", i)

def key_bin(k):
    if isinstance(k, (int, long)):
        return struct.pack("> I", k)
    elif isinstance(k, str):
        return k
    elif isinstance(k, unicode):
        return k.encode("utf-8")
    else:
        raise ValueError("Invalid key type")

class BlockHeader(object): # pylint: disable=too-few-public-methods
    """
    Simple struct class to represent blocksize.
    """

    fmt = struct.Struct("< I I I")
    size = fmt.size

    def __init__(self, raw_size, comp_size, chksum):
        self.raw_size = raw_size
        self.comp_size = comp_size
        self.chksum = chksum

    @classmethod
    def frombuf(cls, buf):
        vals = cls.fmt.unpack_from(buf)
        return cls(*vals)

    def __str__(self):
        return BlockHeader.fmt.pack(self.raw_size, self.comp_size, self.chksum)

    def __repr__(self):
        fmt = "BlockHeader(%d, %d, %0x)"
        return fmt % (self.raw_size, self.comp_size, self.chksum)

def load_block(env, block_db, decomp_fn, i):
    """
    Load a block from the given file.
    """

    i_bin = idx_bin(i)

    with env.begin(block_db, buffers=True) as txn:
        buf = txn.get(i_bin)
        header = BlockHeader.frombuf(buf)

        block_comp = buffer(buf, BlockHeader.size, header.comp_size)
        block_raw = decomp_fn(block_comp)
        block_chksum = checksum(block_raw)

    if block_chksum != header.chksum:
        raise IOError("Block %d: Checksum mismatch: %0x != %0x"
                      % (i, block_chksum, header.chksum))

    block = unpack(block_raw)
    return block

def dump_block(env, block_db, key_db, comp_fn, comp_level, i, block, keys):
    """
    Write the block to the store.
    """

    # Dont write empty blocks
    # This happens when current block is empty and file is closed.
    if not block:
        return

    i_bin = idx_bin(i)

    block_raw = pack(block)
    block_chksum = checksum(block_raw)
    block_comp = comp_fn(block_raw, comp_level)

    header = BlockHeader(len(block_raw), len(block_comp), block_chksum)
    buf = str(header) + block_comp

    with env.begin(block_db, write=True) as txn:
        txn.put(i_bin, buf)

    # Dont start key transaction if not necessary
    if not keys:
        return

    with env.begin(key_db, write=True) as txn:
        for key, idx in keys:
            key, idx = key_bin(key), pack(idx)
            txn.put(key, idx)

class DatasetReader(object):
    """
    Read entries from a dataset file.
    """

    def __init__(self, fname, lock=True):
        self.closed = True

        self.fname = fname

        params = dict(DEFAULT_PARAMS)
        params.update({"readonly": True, "lock": lock})
        self.env = lmdb.open(self.fname, **params)
        try:
            self.meta_db = self.env.open_db("meta", create=False)
            self.key_db = self.env.open_db("key", create=False)
            self.block_db = self.env.open_db("block", create=False)

            with self.env.begin(self.meta_db) as txn:
                version = unpack(txn.get("version"))
                if version != VERSION:
                    raise IOError("Invalid version: %d" % version)

                self.block_length = unpack(txn.get("block_length"))
                self.length = unpack(txn.get("length"))

                self.comp_format = unpack(txn.get("comp_format"))
                self.comp_level = unpack(txn.get("comp_level"))

                try:
                    self.decomp_fn = DECOMP_TABLE[self.comp_format]
                except KeyError:
                    raise IOError("Unknown compression: %s" % self.comp_format)

            self.closed = False
        except:
            self.env.close()
            raise

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    def close(self):
        if not self.closed:
            self.env.close()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        return load_block(self.env, self.block_db, self.decomp_fn, i)

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

    def get_key(self, k):
        """
        Get the value associated with the given key.
        """

        k_bin = key_bin(k)

        with self.env.begin(self.key_db) as txn:
            i = unpack(txn.get(k_bin))

        return self.get_idx(i)

class DatasetWriter(object):
    """
    Writes a dataset object to a file.
    """

    def __init__(self, fname, block_length=1, comp_format="lz4", comp_level=6):
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

        params = dict(DEFAULT_PARAMS)
        params.update({"readonly": False, "lock": True})
        self.env = lmdb.open(self.fname, **params)
        try:
            self.meta_db = self.env.open_db("meta")
            self.key_db = self.env.open_db("key")
            self.block_db = self.env.open_db("block")

            with self.env.begin(self.meta_db) as txn:
                version = unpack_default(txn.get("version"), VERSION)
                if version != VERSION:
                    raise IOError("Invalid version: %d" % version)

                self.block_length = unpack_default(txn.get("block_length"), block_length)
                self.length = unpack_default(txn.get("length"), 0)

                self.comp_format = unpack_default(txn.get("comp_format"), comp_format)
                self.comp_level = unpack_default(txn.get("comp_level"), comp_level)

                self.comp_fn = COMP_TABLE[self.comp_format]
                self.decomp_fn = DECOMP_TABLE[self.comp_format]

            self.write_meta()

            self.closed = False
        except:
            self.env.close()
            raise

        self.cur_block_idx = self.length // self.block_length
        if self.length % self.block_length != 0:
            self.cur_block = self.load_block(self.cur_block_idx)
            self.cur_block_idx -= 1
        else:
            self.cur_block = []
        self.cur_keys = []

    def write_meta(self):
        """
        Write meta information.
        """

        with self.env.begin(self.meta_db, write=True) as txn:
            txn.put("version", pack(VERSION))

            txn.put("block_length", pack(self.block_length))
            txn.put("length", pack(self.length))

            txn.put("comp_format", pack(self.comp_format))
            txn.put("comp_level", pack(self.comp_level))

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        return load_block(self.env, self.block_db, self.decomp_fn, i)

    def dump_block(self, i, block, keys):
        """
        Write the block to the store.
        """

        dump_block(self.env, self.block_db, self.key_db, self.comp_fn, self.comp_level, i, block, keys)

    def flush(self, force=False):
        """
        Flush the current block to output file.
        """

        if len(self.cur_block) != self.block_length and not force:
            raise ValueError("Cant flush unfilled block without forcing")

        if not self.cur_block:
            return

        # Dump the current block and the keys
        self.dump_block(self.cur_block_idx, self.cur_block, self.cur_keys)

        self.cur_block_idx += 1
        self.cur_block = []
        self.cur_keys = []

    def append(self, obj, key=None):
        """
        Append the object to database.
        """

        self.cur_block.append(pack(obj))
        if key is not None:
            self.cur_keys.append((key, self.length))
        self.length += 1

        if len(self.cur_block) == self.block_length:
            self.flush()

    def close(self):
        if not self.closed:
            self.flush(force=True)
            self.write_meta()

            self.env.close()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

def open(fname, mode="r", block_length=None, comp_format="lz4", comp_level=6):
    # pylint: disable=redefined-builtin
    """
    Open a dataset for reading or writing.
    """

    if mode == "r":
        return DatasetReader(fname)
    elif mode == "w" or mode == "a":
        if block_length is None:
            raise ValueError("Must specify block_length for write mode")
        return DatasetWriter(fname, block_length, comp_format, comp_level)
    else:
        raise ValueError("Invalid mode '%s'" % mode)
