from setuptools import setup

setup(
    name="PBDset",
    version="1.0.0a1",

    py_modules=["pbdset", "test_pbdset"],

    install_requires=[
        "msgpack-python",
        "lmdb",
        "lz4",
        "backports.lzma",
        "zstandard"
    ],

    setup_requires=['pytest-runner'],

    # Use dev_requirements.txt instead
    # tests_require=['pytest', 'pytest-cov'],

    author="Parantapa Bhattacharya",
    author_email="pb [at] parantapa [dot] net",

    description="PB's Dataset file format",

    license="MIT",
)
