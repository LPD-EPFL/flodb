// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#ifndef SKIPLIST_CACHE_
#define SKIPLIST_CACHE_

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "db/skiplist-multiinsert/intset.h"


namespace leveldb {

class SLCache {

 private:
  sl_intset_t* sl_; 
  int insertions;
  int evictions;
  void (*deleter_)(uint64_t key, void* value);

 public:
  explicit SLCache(size_t capacity, void (*deleter)(uint64_t key, void* value)) {

      sl_ = sl_set_new();
      deleter_ = deleter;
      insertions = 0;
      evictions = 0;
  }

  void SLCacheThreadInit(int id) {
  }

  virtual ~SLCache() { 
  }

  virtual void* Insert(const uint64_t key, void* value) {
    uint64_t old_val = sl_add(sl_, key, (uint64_t) value, 0);

    if (old_val != 0) {
      evictions ++;
      if (evictions % 100 == 0) {
        printf("%d evictions\n", evictions);
      }
    } else {
      insertions++;
      if (insertions % 1000 == 0) {
        printf("%d insertions\n", insertions);
      }
    }
    return value;
  }


  virtual void* Lookup(const uint64_t key) {
    uint64_t res = sl_contains(sl_, key);
    if (res == 0) {
      return NULL;
    } else {
      return (void*) res;
    }
  }

  virtual void Erase(const uint64_t key) {
    uint64_t res = sl_remove(sl_, key);
    if (res != 0) {
      deleter_(key, (void *)res);
    }
  }

};

}  // namespace leveldb

#endif // SKIPLIST_CACHE_