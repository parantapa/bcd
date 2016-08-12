# Blockwise Compressed Data

BCD is a data format for storing
individually compressed blocks of data.

This is helpful when processing large files parallely,
or when trying to seek to a part of the compressed file.

This project contains two modules:
bcd.py - the base data container format
dset.py - a dataset storate format which uses bcd
