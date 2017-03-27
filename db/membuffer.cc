#include "db/membuffer.h"
#include "db/dbformat.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {



MemBuffer::MemBuffer(uint64_t ht_threshold)
    : refs_(0) {
      buffer_ = new Buffer(ht_threshold);
}


MemBuffer::~MemBuffer() {
  assert(refs_ == 0);
  delete buffer_;
}

MemBufferIterator* MemBuffer::NewIterator() {
  return new MemBufferIterator(buffer_);
}

bool MemBuffer::Add(ValueType type,
                   const Slice& key,
                   const Slice& value) {

  // uint64_t sv = PackSequenceAndType((uint64_t)s, type);
  if (type == kTypeValue) {
    return buffer_->Insert(key, value);
  } else {
    return buffer_->Remove(key);
  }
}


bool MemBuffer::Get(const LookupKey& key, std::string* value, Status* s) {
  return buffer_->Contains(key.user_key(), value, s);
  
}

bool MemBuffer::RemoveIfSameValue(uint64_t key, uint64_t value) {
  return buffer_->RemoveIfSameValue(key, value);
}


}  // namespace leveldb
