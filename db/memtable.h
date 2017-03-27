// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"

#if ASCY_MEMTABLE == 1
#include "db/skiplist-multiinsert.h"
#elif ASCY_MEMTABLE == 2
#include "db/skiplist-herlihy.h"
#elif ASCY_MEMTABLE == 3
#include "db/skiplist-multiinsert.h"
#else 
#include "db/skiplist.h"
#endif
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class Mutex;
class MemTableIterator;

class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  void Delete();
  
  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();


  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

#ifdef ASCY_MEMTABLE
  void Add(SequenceNumber s, ValueType type,
                   const uint64_t key,
                   const uint64_t value);
#endif // ASCY_MEMTABLE  

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

#if ASCY_MEMTABLE == 3
  void MultiInsert(uint64_t* keys, uint64_t* values, uint64_t* seqs, size_t numInsertedKeys);
#endif 

#ifdef ASCY_MEMTABLE
  public:
  ~MemTable();
  private: 
#else
  private:
  ~MemTable();  // Private since only Unref() should be used to delete it
#endif // ASCY_MEMTABLE

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

#if ASCY_MEMTABLE == 1
  typedef SkiplistMultiinsert Table;
#elif ASCY_MEMTABLE == 2
  typedef SkiplistHerlihy Table;
#elif ASCY_MEMTABLE == 3
  typedef SkiplistMultiinsert Table;
#else
  typedef SkipList<const char*, KeyComparator> Table;
#endif



  KeyComparator comparator_;
  int refs_;
  Arena arena_;

  Table table_;

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
