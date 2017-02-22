from setuptools import setup

setup(
    name="PBDset",
    version="1.0.0a1",

    py_modules=["pbdset", "pbdset_tools", "test_pbdset"],

    install_requires=[
        "msgpack-python",
        "lmdb",
        "lz4",
        "backports.lzma",
        "zstandard",
        "Click",
    ],

    entry_points="""
        [console_scripts]
        dset-import=pbdset_tools:import_
    """,

    setup_requires=['pytest-runner'],

    # Use dev_requirements.txt instead
    # tests_require=['pytest', 'pytest-cov'],

    author="Parantapa Bhattacharya",
    author_email="pb [at] parantapa [dot] net",

    description="PB's Dataset file format",

    license="MIT",
)
