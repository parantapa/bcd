#!/usr/bin/env python2
# encoding: utf-8
"""
Tools to convert to/from dset files and viewing them.
"""

import gzip
import bz2
import json

import click
import msgpack

import pbdset

def json_reader(ifnames, opener):
    """
    Read compressed json files.
    """

    for ifname in ifnames:
        click.echo("Reading from: %s" % ifname)
        try:
            with opener(ifname) as fobj:
                for line in fobj:
                    if not line.strip(): continue

                    try:
                        yield json.loads(line)
                    except ValueError as e:
                        msg = "Error parsing json: ValueError(%s)\n%s"
                        msg = msg % (e, line)
                        click.echo(msg, err=True)
        except IOError as e:
            msg = "Error while reading: IOError(%s)"
            msg = msg % e
            click.echo(msg, err=True)

def msgpack_reader(ifnames, opener):
    """
    Read compressed msgpack files.
    """

    for ifname in ifnames:
        click.echo("Reading from: %s" % ifname)
        try:
            with opener(ifname) as fobj:
                unpacker = msgpack.Unpacker(fobj, encoding="utf-8")
                for obj in unpacker:
                    yield obj
        except IOError as e:
            msg = "Error while reading: IOError(%s)"
            msg = msg % e
            click.echo(msg, err=True)

@click.command("import")
@click.option("-i", "--input-format", required=True,
              type=click.Choice(["json.gzip", "json.bz2", "msgpack.gzip"]),
              help="Input file format")
@click.option("-b", "--block-length", default=100,
              help="Number of items in each block")
@click.option("-c", "--compression", default="lz4",
              type=click.Choice(pbdset.COMP_TABLE),
              help="Compression format to use")
@click.argument('ifnames', nargs=-1,
                type=click.Path(exists=True, dir_okay=False))
@click.argument('ofname', nargs=1, type=click.Path())
def import_(input_format, block_length, compression, ifnames, ofname):
    """
    Import data files into a dset archive.
    """

    if input_format.endswith("bz2"):
        opener = lambda f: bz2.BZ2File(f, mode="r")
    else: # assume gzip
        opener = gzip.open

    if input_format.startswith("msgpack"):
        reader = msgpack_reader
    else: # assume json
        reader = json_reader

    params = { "block_length": block_length, "comp_format": compression }
    with pbdset.open(ofname, "w", **params) as dset:
        dset.extend(reader(ifnames, opener))
