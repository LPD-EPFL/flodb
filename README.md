# How to setup on a new machine

ASCYLIB includes a default configuration that uses gcc and tries to infer the number of cores and the frequency of the target/build platform. If this configuration is incorrect, you can always create a manual configurations in `Makefile.common.ascylib`, `db/ascylib_aux/utils.h` and `db/db_test.cc` (look in these files for examples). If you do not care about pinning threads to cores, these settings do not matter. You can compile with make `SET_CPU=0` ... to disable thread pinning.

Example entry for a new platform in `Makefile.common.ascylib`. Replace `lpd48core` with the output of `uname -n`.

    ifeq ($(PC_NAME), lpd48core)
        PLATFORM_KNOWN = 1
        CC = gcc-4.8
        CXXFLAGS += -DOPTERON -DPLATFORM_MCORE
        PLATFORM_NUMA = 1
    endif

Example entry for a new platform in `db/ascylib_aux/utils.h`.

    #if defined(OPTERON)
    #  define NUMBER_OF_SOCKETS 8
    #  define CORES_PER_SOCKET 6
    #  define CACHE_LINE_SIZE 64
    #  define NOP_DURATION 2
      static uint8_t  UNUSED the_cores[] = {
        0, 1, 2, 3, 4, 5, 6, 7, 
        8, 9, 10, 11, 12, 13, 14, 15, 
        16, 17, 18, 19, 20, 21, 22, 23, 
        24, 25, 26, 27, 28, 29, 30, 31, 
        32, 33, 34, 35, 36, 37, 38, 39, 
        40, 41, 42, 43, 44, 45, 46, 47  
      };
    #endif  /*  */

Example entry for a new platform in `db/db_test.cc`. The `TEST_TMPDIR` variable defines the location of the data store files on the current machine:

    #if defined(OPTERON)
      setenv("TEST_TMPDIR", "/run/shm/flodbtest", true);  
    #endif


# Compilation Flags

- `ASCY_MEMTABLE=1` : use the herlihy skiplist without multiinsert (move entries from HT to SL one at a time)
               `=3` : use the herlihy skiplist with multiinsert (move one bucket from SL to HT at a time)
- `USE_HT=0` : disable the hashtable and use just the skiplist (default is to use both)
- `DUMMY_PERSIST=1` : make the persisting thread discard immutable skiplists instead of writing them to disk
- `MSL=x` : give `x` MB to the skiplist
- `MHT=x` : reserve a `x`-million-element hashtable; 8 million corresponds to 256 MB
- `COLLECT_STATS=1` : enable printing statistics at the end of the test (default is off)
- `INIT=seq` : initialize the database with keys in sequential order before starting the experiment (default is no initialization. No initialization is useful when a previously saved database exists, to avoid repopulating the database every time. To use a previously saved database, the path to the database needs to be provided in db_test.cc, as described in “How to setup on a new machine” )
      `=random` : initialize the database with keys in random order before starting the experiment (default is no initialization)
- `N_DRAINING_THREADS=x` : use `x` threads to drain data from HT to SL (default is no draining threads, recommended is 1 draining thread)
- `SKEW=x` : draw keys from a skewed distribution: `x%` of the keys are accessed `(100-x)%` of the time (default is uniform random distribution)
- `ONEWRITER=1` : one thread writes while the other threads read (default is for all threads to have the same op. distribution)
- `CLHT_K=x` : use `x` bits from the key to determine HT bucket (default x = 16)
- `SCANS=1` : use scans instead of reads for the test (default is reads)
- `VERSION=   DEBUG` : compile with `O0` and debug flags
              `SYMBOL` :  compile with `O3` and `-g`
              `O0` : compile with `-O0`
              `O1` : compile with `-O1`
              `O2` : compile with `-O2`
              `O3` : compile with `-O3` (default)


## How to run

1. Compile with desired compilation flags (see above). Run `make db_test` with the desired flags. Example to compile FloDB with a 32 MB hash table and a 96 MB skiplist, multiinsert enabled, 1 draining thread between hash table and skiplist, statistics printing enabled and random initialization.

2. Run tests with the desired parameters. The FloDB tests are included in the file db_test.cc. For example, our tests (as described in our paper) are contained in IgorMultiThreaded. This test can take several parameters, as in the example below:

    `./db_test -n$thd -i$list_size -u$update -d$duration -w$post_init_wait`

where:

  - `n` is the number of worker threads
  - `i` is the initial number of keys in the data store (half the inital range --- will be rounded up to next power of two). The value should be at least `2^(k+1)`, where `k` is the value of the `CLHT_K` compilation parameter.
  - `u` is the percentage of updates (equally distributed between inserts and deletes)
  - `d` is the duration of the test after intialization (in milliseconds)
  - `w` is the time to wait after the db intialization (in seconds)

Example workflow:

Initialize the database:
Set the desired path of the database files in `db_test.cc`.
Compile with the INIT flag set (e.g., INIT=random)

    `make db_test MHT=1 MSL=96 ASCY_MEMTABLE=3 N_DRAINING_THREADS=1  COLLECT_STATS=1 INIT=random`

To initialize for later experiments, run with 0% updates, a short test duration (e.g., -d1), a long post-initialization wait to allow enough time for all compactions to finish (e.g., -w3000) 

    ./db_test -n1 -i200000000 -u0 -d1 -w3000

Run tests from existing database:
Leave the same path as for the initialization run
Compile with the INIT flag not set

    `make clean; make db_test MHT=1 MSL=96 ASCY_MEMTABLE=3 N_DRAINING_THREADS=1  COLLECT_STATS=1`

(Optional) Make a backup of the database to a different location so that the same initialized db can be reused in later experiments
Run `db_test` with a short post-initialization wait (e.g., -w1), with the same value for the initial number of keys in the store (IMPORTANT) and with the other parameters as desired 

    ./db_test -n16 -i200000000 -u50 -d10000 -w1


# LevelDB Original README

**LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values.**

Authors: Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com)

# Features
  * Keys and values are arbitrary byte arrays.
  * Data is stored sorted by key.
  * Callers can provide a custom comparison function to override the sort order.
  * The basic operations are `Put(key,value)`, `Get(key)`, `Delete(key)`.
  * Multiple changes can be made in one atomic batch.
  * Users can create a transient snapshot to get a consistent view of data.
  * Forward and backward iteration is supported over the data.
  * Data is automatically compressed using the [Snappy compression library](http://code.google.com/p/snappy).
  * External activity (file system operations etc.) is relayed through a virtual interface so users can customize the operating system interactions.
  * [Detailed documentation](http://htmlpreview.github.io/?https://github.com/google/leveldb/blob/master/doc/index.html) about how to use the library is included with the source code.


# Limitations
  * This is not a SQL database.  It does not have a relational data model, it does not support SQL queries, and it has no support for indexes.
  * Only a single process (possibly multi-threaded) can access a particular database at a time.
  * There is no client-server support builtin to the library.  An application that needs such support will have to wrap their own server around the library.

# Performance

Here is a performance report (with explanations) from the run of the
included db_bench program.  The results are somewhat noisy, but should
be enough to get a ballpark performance estimate.

## Setup

We use a database with a million entries.  Each entry has a 16 byte
key, and a 100 byte value.  Values used by the benchmark compress to
about half their original size.

    LevelDB:    version 1.1
    Date:       Sun May  1 12:11:26 2011
    CPU:        4 x Intel(R) Core(TM)2 Quad CPU    Q6600  @ 2.40GHz
    CPUCache:   4096 KB
    Keys:       16 bytes each
    Values:     100 bytes each (50 bytes after compression)
    Entries:    1000000
    Raw Size:   110.6 MB (estimated)
    File Size:  62.9 MB (estimated)

## Write performance

The "fill" benchmarks create a brand new database, in either
sequential, or random order.  The "fillsync" benchmark flushes data
from the operating system to the disk after every operation; the other
write operations leave the data sitting in the operating system buffer
cache for a while.  The "overwrite" benchmark does random writes that
update existing keys in the database.

    fillseq      :       1.765 micros/op;   62.7 MB/s
    fillsync     :     268.409 micros/op;    0.4 MB/s (10000 ops)
    fillrandom   :       2.460 micros/op;   45.0 MB/s
    overwrite    :       2.380 micros/op;   46.5 MB/s

Each "op" above corresponds to a write of a single key/value pair.
I.e., a random write benchmark goes at approximately 400,000 writes per second.

Each "fillsync" operation costs much less (0.3 millisecond)
than a disk seek (typically 10 milliseconds).  We suspect that this is
because the hard disk itself is buffering the update in its memory and
responding before the data has been written to the platter.  This may
or may not be safe based on whether or not the hard disk has enough
power to save its memory in the event of a power failure.

## Read performance

We list the performance of reading sequentially in both the forward
and reverse direction, and also the performance of a random lookup.
Note that the database created by the benchmark is quite small.
Therefore the report characterizes the performance of leveldb when the
working set fits in memory.  The cost of reading a piece of data that
is not present in the operating system buffer cache will be dominated
by the one or two disk seeks needed to fetch the data from disk.
Write performance will be mostly unaffected by whether or not the
working set fits in memory.

    readrandom   :      16.677 micros/op;  (approximately 60,000 reads per second)
    readseq      :       0.476 micros/op;  232.3 MB/s
    readreverse  :       0.724 micros/op;  152.9 MB/s

LevelDB compacts its underlying storage data in the background to
improve read performance.  The results listed above were done
immediately after a lot of random writes.  The results after
compactions (which are usually triggered automatically) are better.

    readrandom   :      11.602 micros/op;  (approximately 85,000 reads per second)
    readseq      :       0.423 micros/op;  261.8 MB/s
    readreverse  :       0.663 micros/op;  166.9 MB/s

Some of the high cost of reads comes from repeated decompression of blocks
read from disk.  If we supply enough cache to the leveldb so it can hold the
uncompressed blocks in memory, the read performance improves again:

    readrandom   :       9.775 micros/op;  (approximately 100,000 reads per second before compaction)
    readrandom   :       5.215 micros/op;  (approximately 190,000 reads per second after compaction)

## Repository contents

See doc/index.html for more explanation. See doc/impl.html for a brief overview of the implementation.

The public interface is in include/*.h.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Guide to header files:

* **include/db.h**: Main interface to the DB: Start here

* **include/options.h**: Control over the behavior of an entire database,
and also control over the behavior of individual reads and writes.

* **include/comparator.h**: Abstraction for user-specified comparison function. 
If you want just bytewise comparison of keys, you can use the default
comparator, but clients can write their own comparator implementations if they
want custom ordering (e.g. to handle different character encodings, etc.)

* **include/iterator.h**: Interface for iterating over data. You can get
an iterator from a DB object.

* **include/write_batch.h**: Interface for atomically applying multiple
updates to a database.

* **include/slice.h**: A simple module for maintaining a pointer and a
length into some other byte array.

* **include/status.h**: Status is returned from many of the public interfaces
and is used to report success and various kinds of errors.

* **include/env.h**: 
Abstraction of the OS environment.  A posix implementation of this interface is
in util/env_posix.cc

* **include/table.h, include/table_builder.h**: Lower-level modules that most
clients probably won't use directly
