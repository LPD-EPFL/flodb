// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
#ifdef MSL
      skiplist_size((size_t)(MSL)*1024*1024),
#else
      skiplist_size(128<<20),
#endif
#ifdef MHT
      ht_threshold((size_t)(MHT)*1024*1024),
#else
      ht_threshold(1<<20),
#endif
// #ifdef BIGMEM
//       skiplist_size(1<<30),
// #elif defined(MEDMEM)
//       skiplist_size(512<<20),
// #else
//       skiplist_size(128<<20),
// #endif
      bg_thread_sleep_micros_max(1000),
// #ifdef BIGMEM
//       ht_threshold(8<<20),
// #elif defined(MEDMEM)
//       ht_threshold(4<<20),
// #else
//       ht_threshold(1<<20),
// #endif
      max_open_files(35000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL) {
}


}  // namespace leveldb
