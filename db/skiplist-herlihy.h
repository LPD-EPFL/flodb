// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_HERLIHY_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_HERLIHY_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkiplistHerlihy will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkiplistHerlihy is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkiplistHerlihy.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <iostream>
#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
#include "db/skiplist_herlihy_multiinsert/intset.h"
#include <vector>


namespace leveldb {


// static std::vector<void*> * destroyedSkiplist = new std::vector<void*>();
// class Arena;

// template<typename Key, class Comparator>
class SkiplistHerlihy {
 private:
  // struct Node;
  sl_intset_t* set;

 public:
  // Create a new SkiplistHerlihy object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkiplistHerlihy() {
    size_pad_32 = sizeof(sl_node_t) + ((levelmax+1) * sizeof(sl_node_t *));
    while (size_pad_32 & 31)
      {
        size_pad_32++;
      }
    set = sl_set_new();
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Slice& key, const Slice& val, uint64_t seq);
  void Insert(const uint64_t key, const uint64_t val, uint64_t seq);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Slice& key, std::string* value, Status *s);

  void Remove(const Slice& key, uint64_t seq);
  void Remove(const uint64_t key, uint64_t seq);

  void MultiInsert(uint64_t* keys, uint64_t* values, uint64_t* seqs, size_t numInsertedKeys);


  void Destroy() {
    sl_set_delete(set);
  }

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkiplistHerlihy* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Slice key() const;

    const Slice value() const;

    const uint64_t seq() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(skey_t target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

  private:
    sl_node_t* node_;
    sl_intset_t* set_;
    std::string* scratch_;
    char* valbuf_;
    Slice key_slice_;
    // Intentionally copyable
  };

 private:
  // No copying allowed
  SkiplistHerlihy(const SkiplistHerlihy&);
  void operator=(const SkiplistHerlihy&);
};

static inline Slice NodeKeyToInternalKeySlice(sl_node_t* n) {
  std::string* scratch = new std::string();
  AssignIntToString(scratch, n->key);
  PutFixed64(scratch, n->seq);
  return Slice(*scratch);
}

inline SkiplistHerlihy::Iterator::Iterator(const SkiplistHerlihy* list) {
  node_ = NULL;
  set_ = list->set;
  scratch_ = new std::string();
  valbuf_ = new char[8];
}

inline bool SkiplistHerlihy::Iterator::Valid() const {
  return (node_ != NULL && node_->key != KEY_MAX);
}

inline const Slice SkiplistHerlihy::Iterator::key() const {
  return NodeKeyToInternalKeySlice(node_);
  //AssignIntToString(scratch_, node_->key);
  //PutFixed64(scratch_, node_->seq);
  //// key_slice_.assign(scratch_->data(), scratch_->size());
  //return Slice(*scratch_);
}

inline const Slice SkiplistHerlihy::Iterator::value() const {
  if (node_->val == TOMBSTONE_VALUE) {
    Slice valslice;
    IntToSlice(valslice, valbuf_, node_->val);
    return valslice;
  }
  return *((Slice*) node_->val);
}

inline const uint64_t SkiplistHerlihy::Iterator::seq() const {
  return node_->seq;
}

inline void SkiplistHerlihy::Iterator::Next() {
  node_ = node_->next[0];
}

inline void SkiplistHerlihy::Iterator::Prev() {
  node_ = NULL; // force invalid
}

inline void SkiplistHerlihy::Iterator::Seek(skey_t target) {
  while(node_!=NULL && node_->next[0] != NULL) {
    if (node_->key == target) {
      return;
    }
    node_ = node_->next[0];
  }
}

inline void SkiplistHerlihy::Iterator::SeekToFirst() {
  node_ = set_->head->next[0];
}

inline void SkiplistHerlihy::Iterator::SeekToLast() {
  while(node_!=NULL && node_->next[0] != NULL) {
    node_ = node_->next[0];
  }
}

inline void SkiplistHerlihy::Insert(const Slice& key, const Slice& value, uint64_t seq) {
  // std::cout << "SkiplistHerlihy::Insert " << SliceToInt(key) << std::endl;
  // sl_herlihy_add(set, SliceToInt(key), SliceToInt(value), seq);

  uint64_t old_val = sl_herlihy_add(set, SliceToInt(key), (uint64_t) &value, seq);

  if (old_val != 0 && old_val != TOMBSTONE_VALUE) {
    // std::cout << old_val << " skiplist insert" << std::endl;
    // destroyedSkiplist->push_back((void* )old_val);
    ((Slice *) old_val)->destroy();
  }
}


inline void SkiplistHerlihy::Insert(const uint64_t key, const uint64_t value, uint64_t seq) {
  // std::cout << "SkiplistHerlihy::Insert " << SliceToInt(key) << std::endl;
  uint64_t old_val = sl_herlihy_add(set, key, value, seq);

  if (old_val != 0 && old_val != TOMBSTONE_VALUE) {
    // std::cout << old_val << " skiplist insert" << std::endl;
    // destroyedSkiplist->push_back((void* )old_val);
    ((Slice *) old_val)->destroy();
  }
}

inline void SkiplistHerlihy::MultiInsert(uint64_t* keys, uint64_t* values, uint64_t* seqs, size_t numInsertedKeys){
  
  uint64_t replaced_values_array[numInsertedKeys];
  sl_herlihy_multi_insert(set, keys, values, seqs, numInsertedKeys, replaced_values_array);

  //free replaced values. 
//  printf("Replaced values:\n");
  for (int i = 0; i < numInsertedKeys; i++){

    //printf("%lu \n", replaced_values_array[i]);

    if (replaced_values_array[i] != 0 && replaced_values_array[i] != TOMBSTONE_VALUE) {
      //((Slice *) replaced_values_array[i])->destroy();
    }

  }
  //printf("\n");

}

inline bool SkiplistHerlihy::Contains(const Slice& key, std::string* value, Status *s) {
  // std::cout << "SkiplistHerlihy::Contains " << SliceToInt(key);

  sval_t res = sl_herlihy_contains(set, SliceToInt(key));

  if (res == 0) {
    *s = Status::NotFound(Slice());
    return false;
  } else if (res == TOMBSTONE_VALUE) {
    *s = Status::NotFound(Slice());
    return true;
  } else {
    *s = Status::OK();
    Slice* val = (Slice*) get_unmarked_ref(res);
    value->assign(val->data(), val->size());
    return true;
  }
}

inline void SkiplistHerlihy::Remove(const Slice& key, uint64_t seq) {

  uint64_t old_val = sl_herlihy_add(set, SliceToInt(key), TOMBSTONE_VALUE, seq);

  if (old_val != 0 && old_val != TOMBSTONE_VALUE) {
    ((Slice *) old_val)->destroy();
  }
}

inline void SkiplistHerlihy::Remove(const uint64_t key, uint64_t seq) {
  uint64_t old_val = sl_herlihy_add(set, key, TOMBSTONE_VALUE, seq);

  if (old_val != 0 && old_val != TOMBSTONE_VALUE) {

    ((Slice *) old_val)->destroy();
  }
}


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_FRASER_H_
