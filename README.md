# Gosnapraid

Gosnapraid is a Go implementation of the [SnapRAID](https://github.com/snapraid/snapraid) tool, makes use of a different parity strategy.

## Design

Gosnapraid works by creating a memory efficient index of the files in the dataset. This index is then stored efficiently to files that form a BTree keyed by the filepath on disk. In the index, each file is mapped to its metadata and a range of chunks which are monotonically increasing in ID.

Finally, on the parity disk lists of tuples of (chunk ID, disk ID) are stored which form stripes. This data structure must also be loaded into memory.
