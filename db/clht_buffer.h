// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_CLHT_BUFFER_H_
#define STORAGE_LEVELDB_DB_CLHT_BUFFER_H_

#include <iostream>
#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
#include "clht/clht_lb_res.h"
#include "util/coding.h"
#include <vector>
// #include "clht/clht.h"

namespace leveldb {



class CLHTBuffer {
 private:
  clht_t* ht;

 public:

  CLHTBuffer(uint64_t ht_threshold) {
    // TODO Add initialization here
    // Every clht bucket holds 3 elements. We aim to have 2/3 of the buckets full.
    // -> num_buckets = ht_capacity /3 = ht_threshold/2
    ht = clht_create(ht_threshold/2);
  }

  void CLHTBufferThreadInit(int id) {
    clht_gc_thread_init(ht, id);
  }

  ~CLHTBuffer() {
    clht_gc_destroy(ht);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  bool Insert(const Slice& key, const Slice& val);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Slice& key, std::string* value, Status *s);

  bool Remove(const Slice& key);

  bool RemoveIfSameValue(uint64_t key, uint64_t value);

  size_t NumberOfBuckets() {
    return ht->ht->num_buckets;
  }

  int size() {
    return clht_size(ht->ht);
  }

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const CLHTBuffer* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const uint64_t key() const;

    const uint64_t value() const;

    volatile uint64_t* valueAddr() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    void NextBucket();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(uint64_t target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

    void Random();
    bool AtValidKey() const;

    bool MarkAndGet(uint64_t* key, uint64_t* val);

    // Returns the number of keys that were fetched from the bucket
    int MarkAndGetBucket(uint64_t* keys, uint64_t* vals); 
    int RemoveIfSameValuesBucket(uint64_t* keys, uint64_t* values, size_t nkeys);

    void AdvanceBucket();
    bool AtFirstBucket() const;

    size_t RandomBucket();
    size_t WhichBucket();

  private:
    clht_t* ht_;
    size_t cur_bucket_index;
    size_t cur_key_index;
    // Intentionally copyable

    void Advance();
    bool ReachedEnd() const;
  };

 private:
  // No copying allowed
  CLHTBuffer(const CLHTBuffer&);
  void operator=(const CLHTBuffer&);
};

// static inline Slice NodeKeyToInternalKeySlice(sl_node_t* n) {
//   std::string scratch;
//   AssignIntToString(&scratch, n->key);
//   PutFixed64(&scratch, n->seq);
//   return Slice(scratch);
// }

inline void CLHTBuffer::Iterator::Advance() {
  cur_key_index++;
  if (cur_key_index >= ENTRIES_PER_BUCKET) {
    cur_key_index = 0;
    cur_bucket_index++;
    if (cur_bucket_index >= ht_->ht->num_buckets) {
      cur_bucket_index = 0;
    }
  }
}


inline void CLHTBuffer::Iterator::NextBucket() {

  size_t start_bucket_index = cur_bucket_index;
  while (true) {
    cur_bucket_index++;
    if (cur_bucket_index >= ht_->ht->num_buckets) {
      cur_bucket_index = 0;
    }

    if (ht_->ht->table[cur_bucket_index].key[0] != 0) {
      break;
    } 

    // In this way, if we go through the entire buffer without
    // finding anything, we will return
    if (cur_bucket_index == start_bucket_index) {
      break;
    }
  }
}



inline void CLHTBuffer::Iterator::AdvanceBucket() {
  // if (cur_bucket_index % 100000 == 0) {
  //   printf("At bucket %zu\n", cur_bucket_index);
  // }
  cur_bucket_index++;
  if (cur_bucket_index >= ht_->ht->num_buckets) {
    cur_bucket_index = 0;
  }

}

inline bool CLHTBuffer::Iterator::AtFirstBucket() const {
  return (cur_bucket_index == 0);
}

inline bool CLHTBuffer::Iterator::ReachedEnd() const {
  return (cur_bucket_index >= ht_->ht->num_buckets);
}

inline bool CLHTBuffer::Iterator::AtValidKey() const {
  return (ht_->ht->table[cur_bucket_index].key[cur_key_index] != 0);
}

inline CLHTBuffer::Iterator::Iterator(const CLHTBuffer* clhtbuf) {
  ht_ = clhtbuf->ht;
  cur_bucket_index = 0;
  cur_key_index = 0;
}

inline bool CLHTBuffer::Iterator::Valid() const {
  // return (node_ != NULL && node_->key != UINT64_MAX);
  return (!ReachedEnd() && AtValidKey());
}

inline const uint64_t CLHTBuffer::Iterator::key() const {
  // return NodeKeyToInternalKeySlice(node_);
  return ht_->ht->table[cur_bucket_index].key[cur_key_index];
}

inline const uint64_t CLHTBuffer::Iterator::value() const {
  return ht_->ht->table[cur_bucket_index].val[cur_key_index];
}

inline volatile uint64_t* CLHTBuffer::Iterator::valueAddr() const {
  return &(ht_->ht->table[cur_bucket_index].val[cur_key_index]);
}

inline bool CLHTBuffer::Iterator::MarkAndGet(uint64_t* key, uint64_t* val) {
  return clht_mark_and_get(ht_, key, val, cur_bucket_index, cur_key_index);
}

// TODO
inline int CLHTBuffer::Iterator::MarkAndGetBucket(uint64_t* key, uint64_t* val) {
  return clht_mark_and_get_bucket(ht_, key, val, cur_bucket_index);
}

// TODO
inline int CLHTBuffer::Iterator::RemoveIfSameValuesBucket(uint64_t* keys, uint64_t* vals, size_t nkeys) {
  return clht_remove_if_same_values_bucket(ht_, keys, vals, nkeys, cur_bucket_index);
}

inline void CLHTBuffer::Iterator::Next() {
  Advance();
  while (!AtValidKey()) {
    Advance();
  }
}

inline void CLHTBuffer::Iterator::Prev() {
}

inline void CLHTBuffer::Iterator::Seek(uint64_t target) {
}

inline void CLHTBuffer::Iterator::SeekToFirst() {
  while (!ReachedEnd() && !AtValidKey()) {
    Advance();
  }
  // while (cur_bucket_index < ht_->ht->num_buckets && 
  //   ht_->ht->table[cur_bucket_index].key[cur_key_index] == 0) {
  //   Next();
  // }
}

inline void CLHTBuffer::Iterator::SeekToLast() {
}

inline void CLHTBuffer::Iterator::Random() {
  cur_bucket_index = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2]))) % (ht_->ht->num_buckets);
  cur_key_index = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2]))) % (ENTRIES_PER_BUCKET);
}

inline size_t CLHTBuffer::Iterator::RandomBucket() {
  cur_bucket_index = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2]))) % (ht_->ht->num_buckets);
  return cur_bucket_index;
}

inline size_t CLHTBuffer::Iterator::WhichBucket() {
  return cur_bucket_index;
}

inline bool CLHTBuffer::Insert(const Slice& key, const Slice& value) {
  // std::cout << "CLHTBuffer::Insert " << SliceToInt(key) << std::endl;

  uint64_t old_val;
#if ASCY_MEMTABLE == 3
  bool status = clht_put_with_partition(ht, SliceToInt(key), (uint64_t) &value, &old_val);
#else
  bool status = clht_put(ht, SliceToInt(key), (uint64_t) &value, &old_val);
#endif

  if (!status) {
    return false;
  }
  if (old_val != 0 && old_val != TOMBSTONE_VALUE && !is_marked_ref(old_val)) { // not empty and not tombstone and not marked
    // std::cout << old_val << " clht insert" << std::endl;
    ((Slice *) old_val)->destroy();
  }
  return true;
}

inline bool CLHTBuffer::Contains(const Slice& key, std::string* value, Status *s) {
  // std::cout << "CLHTBuffer::Contains " << SliceToInt(key);

  uint64_t res = get_unmarked_ref(clht_get_with_partition(ht->ht, SliceToInt(key)));

  if (res == 0) {
    *s = Status::NotFound(Slice());
    return false;
  } else if (res == TOMBSTONE_VALUE) {
    *s = Status::NotFound(Slice());
    return true;
  } else {
    *s = Status::OK();
    Slice* val = (Slice*) res;
    value->assign(val->data(), val->size());
    // AssignIntToString(value, res);
    return true;
  }
}

inline bool CLHTBuffer::Remove(const Slice& key) {

  uint64_t old_val;

#if ASCY_MEMTABLE == 3
  bool status = clht_put_with_partition(ht, SliceToInt(key), TOMBSTONE_VALUE, &old_val);
#else
  bool status = clht_put(ht, SliceToInt(key), TOMBSTONE_VALUE, &old_val);
#endif

  if (!status) {
    return false;
  }
  if (old_val != 0 && old_val != TOMBSTONE_VALUE && !is_marked_ref(old_val)) {
    // std::cout << old_val << " clht remove" << std::endl;

    ((Slice *) old_val)->destroy();

  }
  return true;
}

inline bool CLHTBuffer::RemoveIfSameValue(uint64_t key, uint64_t value) {

  return clht_remove_if_same(ht, key, value);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_CLHT_BUFFER_H_
