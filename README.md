# Gosnapraid

Gosnapraid is a Go implementation of the [SnapRAID](https://github.com/snapraid/snapraid) tool, makes use of a different parity strategy.

## Design


### Content Manifests

The first thing that gosnapraid does on a sync operation is generate a manifest of the contents of 
each disk in the array. This process is conducted in parallel for all disks. The manifest is a binary
representation of the directory hierarchy written out efficiently to disk. The filemetadata tracked 
includes:
 
  - Inode number
  - Generation number
  - Access time
  - Change time
  - Birth time
  - User ID
  - Group ID
  - Device ID
  - File size
  - File mode
  - File type

The filemetadata is used to determine if a file has changed since the last sync. If the filemetadata 
doesn't match, as a final step the file is hashed and a hash is stored in the metadata. This hash
authoritatively determines whether or not the contents of the file has changed.

The manifest hierarchy is always generated and stored in lexographically sorted order to ensure
that snapshots can be diffed efficiently by simple sorted tree traversal. 

#### Parity Scheme

To implement the parity scheme gosnapraid needs to be able to construct virtual stripes for parity
calculations out of the files in the array.

To accomplish this, a stripe allocator is implemented. The range of strip IDs is equal to the size of the largest available parity disk / chunk size (which is 128 KB by default).

The stripe allocator, keeps track of unmapped stripe IDs for each disk in the array. When a file is modified, its chunks are assigned unmapped stripe IDs for that disk by the stripe allocator.

When running parity calculations, the stripe map is read to map each chunk to the file that backs it. Ideally fragmentation is (reasonably) low, we will keep a pool of ~1000 files open at a time, and will read chunks from them in parallel where possible.

Some active scheme to rebalance chunks may be clever.

