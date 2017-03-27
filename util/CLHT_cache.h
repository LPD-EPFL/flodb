// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#ifndef CLHT_CACHE_
#define CLHT_CACHE_

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "db/clht/clht_lb_res.h"


namespace leveldb {

class CLHTCache {

 private:
  clht_t* clht_;
  int failed_insertions;
  int evictions;
  void (*deleter_)(uint64_t key, void* value);

 public:
  explicit CLHTCache(size_t capacity, void (*deleter)(uint64_t key, void* value)) {

      clht_ = clht_create(capacity);
      deleter_ = deleter;
      failed_insertions = 0;
      evictions = 0;
  }

  void CLHTCacheThreadInit(int id) {
    clht_gc_thread_init(clht_, id);
  }

  virtual ~CLHTCache() { 
    clht_gc_destroy(clht_);
  }

  virtual void* Insert(const uint64_t key, void* value) {
    uint64_t old_val;    
    bool res = clht_put(clht_, key, (uint64_t) value, &old_val);

    if (res == false) {
      failed_insertions ++;
      if (failed_insertions % 100 == 0) {
        printf("%d failed insertions --- ", failed_insertions);
        printf("hashtable size: %zu in %zu buckets\n", clht_size(clht_->ht), clht_->ht->num_buckets);
      }
    } else if (old_val != 0) {
      evictions ++;
      if (evictions % 100 == 0) {
        printf("%d evictions\n", evictions);
      }
    }
    return value;
  }


  virtual void* Lookup(const uint64_t key) {
    uint64_t res = clht_get(clht_->ht, key);
    if (res == 0) {
      return NULL;
    } else {
      return (void*) res;
    }
  }

  virtual void Erase(const uint64_t key) {
    uint64_t res = clht_remove(clht_, key);
    if (res != 0) {
      deleter_(key, (void *)res);
    }
  }

};

}  // namespace leveldb

#endif // CLHT_CACHE_