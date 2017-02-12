from setuptools import setup

setup(
    name="PBDset",
    version="1.0.0a1",

    py_modules=["pbdset", "test_pbdset"],

    install_requires=[
        'msgpack-python',
        'lmdb',
        'lz4',
        'backports.lzma'
    ],

    author="Parantapa Bhattacharya",
    author_email="pb [at] parantapa [dot] net",

    description="PB's Dataset file format",

    license="MIT",
)
