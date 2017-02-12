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

import sys
import struct
import os.path
import traceback
from threading import Thread
from Queue import Queue, Full as QueueFull, Empty as QueueEmpty

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

DEFAULT_PARAMS = {
    "map_size": 2 ** 40, # 1 Terabyte
    "subdir": False,
    "readonly": True,
    "max_dbs": 2,
    "lock": False,
    "sync": False,
    "metasync": False,
}

def idx_bin(i):
    return struct.pack("> I", i)

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

def synchronize_ordering(inq, outq, start_oid, max_window_size, exceptions):
    """
    Synchronize ordering of objects.
    """

    window = {}
    next_oid = start_oid

    while not exceptions:
        obj = inq.get()
        if obj is None:
            break

        oid = obj[0]
        window[oid] = obj

        if next_oid in window:
            obj = window.pop(next_oid)
            outq.put(obj)
            next_oid += 1
        if len(window) > max_window_size:
            e = "Maximum window size exceeded: %d > %d"
            e = e % (len(window), max_window_size)
            raise ValueError(e)

    while window and not exceptions:
        if next_oid not in window:
            raise ValueError("Can not maintain ordering")

        oid = window.pop(next_oid)
        outq.put(oid)
        next_oid += 1

class DatasetWriter(object):
    """
    Writes a dataset object to a file.
    """

    def __init__(self, fname, create=True, block_length=1,
                 comp_format="lz4", comp_level=6, num_workers=1):
        # pylint: disable=too-many-statements
        self.closed = True

        # Check the parameters
        block_length = int(block_length)
        if block_length < 1:
            raise ValueError("Block length must be at-least 1")
        if comp_format not in DECOMP_TABLE:
            raise ValueError("Unknown compression: %s" % comp_format)
        comp_level = int(comp_level)
        if not 1 <= comp_level <= 9:
            raise ValueError("Invalid compression level: %d" % comp_level)
        num_workers = int(num_workers)

        # Check if file exists
        _exists = os.path.exists(fname)
        if create and _exists:
            raise IOError("File '%s' already exists" % fname)
        if not create and not _exists:
            raise IOError("File '%s' doesn't exist" % fname)

        self.fname = fname

        params = dict(DEFAULT_PARAMS)
        params.update({"readonly": False})
        self.env = lmdb.open(self.fname, **params)
        try:
            self.meta_db = self.env.open_db("meta")
            self.block_db = self.env.open_db("block")

            with self.env.begin(self.meta_db) as txn:
                version = unpack(txn.get("version"), VERSION)
                if version != VERSION:
                    raise IOError("Invalid version: %d" % version)

                self.block_length = unpack(txn.get("block_length"), block_length)
                self.length = unpack(txn.get("length"), 0)

                self.comp_format = unpack(txn.get("comp_format"), comp_format)
                self.comp_level = unpack(txn.get("comp_level"), comp_level)

                self.comp_fn = COMP_TABLE[self.comp_format]
                self.decomp_fn = DECOMP_TABLE[self.comp_format]

            if self.length == 0:
                self.write_meta(True)

            self.num_workers = num_workers
            if self.num_workers > 1:
                self.comp_queue = Queue(num_workers)
                self.sync_queue = Queue(1)
                self.write_queue = Queue(1)

                self.thread_exceptions = []

                self.comp_threads = [Thread(target=self.comp_worker)
                                     for _ in xrange(num_workers)]
                self.sync_thread = Thread(target=self.sync_worker)
                self.write_thread = Thread(target=self.write_worker)

                for w in self.comp_threads:
                    w.start()
                self.sync_thread.start()
                self.write_thread.start()

            self.closed = False
        except:
            self.env.close()
            raise

        # number of blocks already present in the dataset
        self.num_blocks  = self.length // self.block_length
        self.num_blocks += bool(self.length % self.block_length)

        if self.length % self.block_length == 0:
            self.cur_block = []
        else:
            self.cur_block = self.load_block(self.num_blocks -1)
            self.num_blocks -= 1

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        i_bin = idx_bin(i)
        with self.env.begin(self.block_db, buffers=True) as txn:
            block_hcomp = txn.get(i_bin)
            if block_hcomp is None:
                raise IOError("Block %d: doesn't exist" % i)

            block_raw = decomp_block(block_hcomp, self.decomp_fn)

        block = unpack(block_raw)
        return block

    def dump_block(self, i, block):
        """
        Write the block to the store.
        """

        block_raw = pack(block)
        if self.num_workers > 1:
            if not self.thread_exceptions:
                self.comp_queue.put((i, block_raw))
            else:
                raise IOError("Some error occured in different thread")
        else:
            block_hcomp = comp_block(block_raw, self.comp_fn, self.comp_level)
            i_bin = idx_bin(i)
            with self.env.begin(self.block_db, write=True) as txn:
                txn.put(i_bin, block_hcomp)
            self.write_meta()

    def comp_worker(self):
        try:
            while not self.thread_exceptions:
                obj = self.comp_queue.get()
                if obj is None:
                    break

                i, block_raw = obj
                block_hcomp = comp_block(block_raw, self.comp_fn, self.comp_level)
                self.sync_queue.put((i, block_hcomp))
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def sync_worker(self):
        start_oid = self.num_blocks
        max_window_size = self.num_workers
        try:
            synchronize_ordering(self.sync_queue, self.write_queue,
                                 start_oid, max_window_size,
                                 self.thread_exceptions)
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def write_worker(self):
        try:
            while not self.thread_exceptions:
                obj = self.write_queue.get()
                if obj is None:
                    break

                i, block_hcomp = obj
                i_bin = idx_bin(i)
                with self.env.begin(self.block_db, write=True) as txn:
                    txn.put(i_bin, block_hcomp)
                self.write_meta()
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def write_meta(self, full=False):
        """
        Write meta information.
        """

        with self.env.begin(self.meta_db, write=True) as txn:
            if full:
                txn.put("version", pack(VERSION))
                txn.put("block_length", pack(self.block_length))
                txn.put("comp_format", pack(self.comp_format))
                txn.put("comp_level", pack(self.comp_level))

            txn.put("length", pack(self.length))

    def flush(self, force=False):
        """
        Flush the current block to output file.
        """

        if len(self.cur_block) != self.block_length and not force:
            raise ValueError("Cant flush unfilled block without forcing")

        if not self.cur_block:
            return

        # Dump the current block
        self.dump_block(self.num_blocks, self.cur_block)

        self.num_blocks += 1
        self.cur_block = []

    def close(self):
        if not self.closed:
            self.flush(force=True)

            if self.num_workers > 1:
                if self.thread_exceptions:
                    for w in self.comp_threads:
                        w.join()
                    self.sync_queue.join()
                    self.write_thread.join()
                else:
                    for _ in xrange(self.num_workers):
                        self.comp_queue.put(None)
                    for w in self.comp_threads:
                        w.join()
                    self.sync_queue.put(None)
                    self.sync_queue.join()
                    self.write_queue.put(None)
                    self.write_thread.join()

            self.env.sync(True)
            self.env.close()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

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

class DatasetParallelCursor(object):
    """
    Decompresses the blocks from the dataset file in parallel.
    """

    def __init__(self, reader, num_workers, ns, bs):
        self.reader = reader
        self.ns = iter(ns)
        self.bs = iter(bs)
        self.num_workers = num_workers

        self.block_length = self.reader.block_length

        self.decomp_queue = Queue(num_workers)
        self.sync_queue = Queue(1)
        self.yield_queue = Queue(1)

        self.reader_thread = Thread(target=self.reader_worker)
        self.decomp_threads = [Thread(target=self.decomp_worker)
                               for _ in xrange(num_workers)]
        self.sync_thread = Thread(target=self.sync_worker)

        self.thread_exceptions = []

        self.reader_thread.start()
        for w in self.decomp_threads:
            w.start()
        self.sync_thread.start()

        self.cur_block_idx = -1
        self.cur_block = None

    @classmethod
    def from_idxs(cls, reader, num_workers, ns):
        """
        Return a cursor for separate index inputs.
        """

        _block_length = reader.block_length

        bs = []
        for n in ns:
            i = n // _block_length
            if bs and bs[-1] == i:
                continue
            bs.append(i)

        return cls(reader, num_workers, ns, bs)

    @classmethod
    def from_slice(cls, reader, num_workers, start, stop, step):
        """
        Return a cursor for slice input.
        """

        _block_length = reader.block_length

        def slice_bs():
            last_b = -1
            for n in xrange(start, stop, step):
                i = n // _block_length
                if i == last_b: continue
                last_b = i
                yield i

        ns = xrange(start, stop, step)
        bs = slice_bs()

        return cls(reader, num_workers, ns, bs)

    def reader_worker(self):
        try:
            for o, i in enumerate(self.bs):
                if self.thread_exceptions:
                    return

                i_bin = idx_bin(i)
                with self.reader.env.begin(self.reader.block_db) as txn:
                    block_hcomp = txn.get(i_bin)
                self.decomp_queue.put((o, i, block_hcomp))
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def decomp_worker(self):
        try:
            while not self.thread_exceptions:
                for o, i, block_hcomp in iter(self.decomp_queue.get, None):
                    block_raw = decomp_block(block_hcomp, self.reader.decomp_fn)
                    self.sync_queue.put((o, i, block_raw))
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def sync_worker(self):
        start_oid = 0
        max_window_size = self.num_workers
        try:
            synchronize_ordering(self.sync_queue, self.yield_queue,
                                 start_oid, max_window_size,
                                 self.thread_exceptions)
        except Exception as e: # pylint: disable=broad-except
            traceback.print_exc(file=sys.stderr)
            self.thread_exceptions.append(e)

    def __iter__(self):
        return self

    def next(self):
        """
        Return the next item from ther iterator.
        """

        try:
            n = self.ns.next()
            i = n // self.block_length
            j = n % self.block_length

            # If the current block is not the one we need
            # the next one should be it
            if self.cur_block_idx != i:
                if self.thread_exceptions:
                    raise IOError("Some error occured in different thread")
                obj = self.yield_queue.get()
                if obj is None:
                    raise IOError("Did not get any block")
                _, self.cur_block_idx, self.cur_block = obj
                if self.cur_block_idx != i:
                    raise IOError("Did not get the correct block")
                self.cur_block = unpack(self.cur_block)

            return unpack(self.cur_block[j])
        except StopIteration:
            self.close(clean=True)
            raise
        except Exception:
            self.close()
            raise

    def close(self, clean=False):
        if clean:
            self.reader_thread.join()
            for w in self.decomp_threads:
                self.decomp_queue.put(None)
            for w in self.decomp_threads:
                w.join()
            self.sync_queue.put(None)
            self.sync_thread.join()
        else:
            self.thread_exceptions.append(None)

            # For anyone waiting on input
            # lets give them content.
            try:
                for w in self.decomp_threads:
                    self.decomp_queue.put_nowait(None)
            except QueueFull:
                pass
            try:
                self.sync_queue.put_nowait(None)
            except QueueFull:
                pass

            # For anyone waiting on output
            # lets clear up some space
            try:
                for w in self.decomp_threads:
                    self.decomp_queue.get_nowait()
            except QueueEmpty:
                pass
            try:
                self.sync_queue.get_nowait()
            except QueueEmpty:
                pass

            # Now everyone should quit on their own
            self.reader_thread.join()
            for w in self.decomp_threads:
                w.join()
            self.sync_thread.join()

    def __del__(self):
        self.close()

class DatasetReader(object):
    """
    Read entries from a dataset file.
    """

    def __init__(self, fname):
        self.closed = True

        if not os.path.exists(fname):
            raise IOError("File '%s' doesn't exist" % fname)

        self.fname = fname

        params = dict(DEFAULT_PARAMS)
        params.update({"readonly": True})
        self.env = lmdb.open(self.fname, **params)
        try:
            self.meta_db = self.env.open_db("meta", create=False)
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

        # number of blocks already present in the dataset
        self.num_blocks  = self.length // self.block_length
        self.num_blocks += bool(self.length % self.block_length)

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    def load_block(self, i):
        """
        Load a block from the given file.
        """

        i_bin = idx_bin(i)
        with self.env.begin(self.block_db, buffers=True) as txn:
            block_hcomp = txn.get(i_bin)
            if block_hcomp is None:
                raise IOError("Block %d: doesn't exist" % i)

            block_raw = decomp_block(block_hcomp, self.decomp_fn)

        block = unpack(block_raw)
        return block

    def close(self):
        if not self.closed:
            self.env.close()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

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

    def get_slice(self, *args, **kwargs):
        """
        Return iterable for the given range.
        """

        start, stop, step = slice(*args).indices(self.length)
        num_workers = kwargs.get("num_workers", 1)

        # Find the number of items in slice
        n = (stop - start) // step
        if n <= 0:
            return []

        # Check if begin and end indexes are in range
        if start < 0 or start >= self.length:
            raise IndexError("Index out of range")
        end = start + (n - 1) * step
        if end < 0 or end >= self.length:
            raise IndexError("Index out of range")

        if num_workers > 1:
            return DatasetParallelCursor.from_slice(self, num_workers, start, stop, step)
        else:
            return self._get_slice_simple(start, stop, step)

    def _get_slice_simple(self, start, stop, step):
        """
        Return iterable for the given range.
        """

        _block_length = self.block_length

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

    def get_idxs(self, ns, **kwargs):
        """
        Get the values at given idxs.

        NOTE: if the indexes are not sorted,
        performance may be really slow.
        """

        num_workers = kwargs.get("num_workers", 1)

        if num_workers > 1:
            return DatasetParallelCursor.from_idxs(self, num_workers, ns)
        else:
            return self._get_idxs_simple(ns)

    def _get_idxs_simple(self, ns):
        """
        Get the values at the given idxs.
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

def open(fname, mode="r", block_length=None,
         comp_format="lz4", comp_level=6, num_workers=1):
    # pylint: disable=redefined-builtin
    """
    Open a dataset for reading or writing.
    """

    if mode == "r":
        return DatasetReader(fname)
    elif mode == "w":
        if block_length is None:
            raise ValueError("Must specify block_length for write mode")
        return DatasetWriter(fname, True, block_length,
                             comp_format, comp_level, num_workers)
    elif mode == "a":
        return DatasetWriter(fname, False, num_workers=num_workers)
    else:
        raise ValueError("Invalid mode '%s'" % mode)
