// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

IntCache::~IntCache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct IntLRUHandle {
  void* value;
  void (*deleter)(const uint64_t, void* value);
  IntLRUHandle* next_hash;
  IntLRUHandle* next;
  IntLRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  uint64_t refs;
  uint64_t key;      // Hash of key(); used for fast sharding and comparisons

};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  IntLRUHandle* Lookup(const uint64_t key) {
    return *FindPointer(key);
  }

  IntLRUHandle* Insert(IntLRUHandle* h) {
    IntLRUHandle** ptr = FindPointer(h->key);
    IntLRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  IntLRUHandle* Remove(const uint64_t key) {
    IntLRUHandle** ptr = FindPointer(key);
    IntLRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint64_t length_;
  uint64_t elems_;
  IntLRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  IntLRUHandle** FindPointer(const uint64_t key) {
    IntLRUHandle** ptr = &list_[key & (length_ - 1)];
    while (*ptr != NULL &&
           (key != (*ptr)->key)) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint64_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    IntLRUHandle** new_list = new IntLRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint64_t count = 0;
    for (uint64_t i = 0; i < length_; i++) {
      IntLRUHandle* h = list_[i];
      while (h != NULL) {
        IntLRUHandle* next = h->next_hash;
        uint64_t key = h->key;
        IntLRUHandle** ptr = &new_list[key & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class IntLRUCache {
 public:
  IntLRUCache();
  ~IntLRUCache();

  // Separate from constructor so caller can easily make an array of IntLRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  IntCache::IntHandle* Insert(const uint64_t key,
                        void* value, size_t charge,
                        void (*deleter)(const uint64_t key, void* value));

  IntCache::IntHandle* Lookup(const uint64_t key);
  void Release(IntCache::IntHandle* handle);
  void Erase(const uint64_t key);

 private:
  void IntLRU_Remove(IntLRUHandle* e);
  void IntLRU_Append(IntLRUHandle* e);
  void Unref(IntLRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_;
  size_t usage_;

  // Dummy head of IntLRU list.
  // IntLRU.prev is newest entry, IntLRU.next is oldest entry.
  IntLRUHandle lru_;

  HandleTable table_;
};

IntLRUCache::IntLRUCache()
    : usage_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

IntLRUCache::~IntLRUCache() {
  for (IntLRUHandle* e = lru_.next; e != &lru_; ) {
    IntLRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void IntLRUCache::Unref(IntLRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key, e->value);
    free(e);
  }
}

void IntLRUCache::IntLRU_Remove(IntLRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void IntLRUCache::IntLRU_Append(IntLRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

IntCache::IntHandle* IntLRUCache::Lookup(const uint64_t key) {
  MutexLock l(&mutex_);
  IntLRUHandle* e = table_.Lookup(key);
  if (e != NULL) {
    e->refs++;
    IntLRU_Remove(e);
    IntLRU_Append(e);
  }
  return reinterpret_cast<IntCache::IntHandle*>(e);
}

void IntLRUCache::Release(IntCache::IntHandle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<IntLRUHandle*>(handle));
}

IntCache::IntHandle* IntLRUCache::Insert(
    const uint64_t key, void* value, size_t charge,
    void (*deleter)(const uint64_t key, void* value)) {
  MutexLock l(&mutex_);

  IntLRUHandle* e = reinterpret_cast<IntLRUHandle*>(
      malloc(sizeof(IntLRUHandle)));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key = key;
  e->refs = 2;  // One from IntLRUCache, one for the returned handle
  IntLRU_Append(e);
  usage_ += charge;

  IntLRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    IntLRU_Remove(old);
    Unref(old);
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    IntLRUHandle* old = lru_.next;
    IntLRU_Remove(old);
    table_.Remove(old->key);
    Unref(old);
  }

  return reinterpret_cast<IntCache::IntHandle*>(e);
}

void IntLRUCache::Erase(const uint64_t key) {
  MutexLock l(&mutex_);
  IntLRUHandle* e = table_.Remove(key);
  if (e != NULL) {
    IntLRU_Remove(e);
    Unref(e);
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedIntLRUCache : public IntCache {
 private:
  IntLRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static uint64_t Shard(uint64_t hash) {
    return hash >> (64 - kNumShardBits);
  }

 public:
  explicit ShardedIntLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedIntLRUCache() { }
  virtual IntHandle* Insert(const uint64_t key, void* value, size_t charge,
                         void (*deleter)(const uint64_t key, void* value)) {
    return shard_[Shard(key)].Insert(key, value, charge, deleter);
  }

  virtual IntHandle* Lookup(const uint64_t key) {
    return shard_[Shard(key)].Lookup(key);
  }
  virtual void Release(IntHandle* handle) {
    IntLRUHandle* h = reinterpret_cast<IntLRUHandle*>(handle);
    shard_[Shard(h->key)].Release(handle);
  }
  virtual void Erase(const uint64_t key) {
    // const uint32_t hash = HashSlice(key);
    shard_[Shard(key)].Erase(key);
  }
  virtual void* Value(IntHandle* handle) {
    return reinterpret_cast<IntLRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
};

}  // end anonymous namespace

IntCache* NewIntLRUCache(size_t capacity) {
  return new ShardedIntLRUCache(capacity);
}

}  // namespace leveldb
