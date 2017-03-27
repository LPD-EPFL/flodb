#ifndef STORAGE_LEVELDB_DB_MEMBUFFER_H_
#define STORAGE_LEVELDB_DB_MEMBUFFER_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/clht_buffer.h"

namespace leveldb {

class InternalKeyComparator;
class MemBufferIterator;

class MemBuffer {
 public:

  MemBuffer(uint64_t ht_threshold);

  void MemBufferThreadInit(int id) {
    buffer_->CLHTBufferThreadInit(id);
  }

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

  // Return an iterator that yields the contents of the MemBuffer.
  //
  // The caller must ensure that the underlying MemBuffer remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  MemBufferIterator* NewIterator();

  // Add an entry into MemBuffer that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  bool Add(ValueType type,
           const Slice& key,
           const Slice& value);  

  // If MemBuffer contains a value for key, store it in *value and return true.
  // If MemBuffer contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

  bool RemoveIfSameValue(uint64_t key, uint64_t value);

  size_t NumberOfBuckets() {
    return buffer_->NumberOfBuckets();
  }

  int size() {
    return buffer_->size();
  }
  
  public:
  ~MemBuffer();
  private: 

  // struct KeyComparator {
  //   const InternalKeyComparator comparator;
  //   explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
  //   int operator()(const char* a, const char* b) const;
  // };
  // friend class MemTableBackwardIterator;

  friend class MemBufferIterator;
  typedef CLHTBuffer Buffer;

  int refs_;
  Buffer* buffer_;

  // No copying allowed
  MemBuffer(const MemBuffer&);
  void operator=(const MemBuffer&);
};

class MemBufferIterator {
 public:
  explicit MemBufferIterator(MemBuffer::Buffer* table) : iter_(table) { }

  MemBufferIterator(MemBuffer* buffer): iter_(buffer->buffer_) {}

  virtual void Seek(const Slice& k) { iter_.Seek(SliceToInt(k)); }
  virtual bool Valid() const { return iter_.Valid(); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void NextBucket() { iter_.NextBucket(); }
  virtual void Prev() { iter_.Prev(); }
  virtual uint64_t key() const { return iter_.key();}
  virtual uint64_t value() const { return iter_.value();} 
  virtual volatile uint64_t* valueAddr() const { return iter_.valueAddr();} 
  virtual Status status() const { return Status::OK(); }
  virtual void RandomEntry() { iter_.Random(); }
  virtual size_t RandomBucket() { return iter_.RandomBucket(); }
  virtual size_t WhichBucket() { return iter_.WhichBucket(); }
  virtual bool AtValidKey() const { return iter_.AtValidKey(); }
  virtual bool AtFirstBucket() const { return iter_.AtFirstBucket(); }
  virtual void AdvanceBucket() { iter_.AdvanceBucket(); }
  virtual bool MarkAndGet(uint64_t* key, uint64_t* val) { return iter_.MarkAndGet(key, val); }
  
  virtual int MarkAndGetBucket(uint64_t* keys, uint64_t* vals) 
    { return iter_.MarkAndGetBucket(keys, vals); }
  virtual int RemoveIfSameValuesBucket(uint64_t* keys, uint64_t* values, size_t nkeys) 
    {return iter_.RemoveIfSameValuesBucket(keys, values, nkeys);}



 private:
  MemBuffer::Buffer::Iterator iter_;

  // No copying allowed
  MemBufferIterator(const MemBufferIterator&);
  void operator=(const MemBufferIterator&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMBUFFER_H_
