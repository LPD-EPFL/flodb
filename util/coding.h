// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef STORAGE_LEVELDB_UTIL_CODING_H_
#define STORAGE_LEVELDB_UTIL_CODING_H_

#include <stdint.h>
#include <string.h>
#include <string>
#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

// Standard Put... routines append to a string
extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint64(std::string* dst, uint64_t value);
extern void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
extern bool GetVarint32(Slice* input, uint32_t* value);
extern bool GetVarint64(Slice* input, uint64_t* value);
extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// NULL on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p,const char* limit, uint32_t* v);
extern const char* GetVarint64Ptr(const char* p,const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed32(char* dst, uint32_t value);
extern void EncodeFixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t DecodeFixed32(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

inline uint64_t first_n_bits(uint64_t key, int n) {
    uint64_t shifted = key >> (64 - n);
    //printf("%p ->(%d)-> %p = %lu\n", (void*) key, n, (void *) shifted, shifted);
    return shifted;
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p,
                                          const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

inline int is_marked_ref(uint64_t i) 
{
  return (i & 0x1L);
}

inline uint64_t get_unmarked_ref(uint64_t w) 
{
  return (w & ~0x1L);
}

inline uint64_t get_marked_ref(uint64_t w) 
{
  return (w | 0x1L);
}

// inline size_t IntLength(int64_t input) {
//   if (input == 0) return 0;
//   if (((input >> 8) & 0xff) == 0) return 1;
//   if (((input >> 16) & 0xff) == 0) return 2;
//   if (((input >> 24) & 0xff) == 0) return 3;
//   if (((input >> 32) & 0xff) == 0) return 4;
//   if (((input >> 40) & 0xff) == 0) return 5;
//   if (((input >> 48) & 0xff) == 0) return 6;
//   if (((input >> 56) & 0xff) == 0) return 7;
//   return 8;
// }

inline size_t IntLength(uint64_t input) {
  if (input < 1UL<<8) return 1;
  if (input < 1UL<<16) return 2;
  if (input < 1UL<<24) return 3;
  if (input < 1UL<<32) return 4;
  if (input < 1UL<<40) return 5;
  if (input < 1UL<<48) return 6;
  if (input < 1UL<<56) return 7;
  return 8;
}

inline void AssignIntToString2(std::string* dst, uint64_t input) {
  size_t size = IntLength(input);
  char buf[size];
  memcpy (buf, &input, size);
  dst->assign(buf, size);
  // dst->assign((char *) &input);
}

inline void AssignIntToString(std::string* dst, uint64_t input) {
  char buf[8];
  buf[7] =  input & 0xff;
  buf[6] = (input >> 8) & 0xff;
  buf[5] = (input >> 16) & 0xff;
  buf[4] = (input >> 24) & 0xff;
  buf[3] = (input >> 32) & 0xff;
  buf[2] = (input >> 40) & 0xff;
  buf[1] = (input >> 48) & 0xff;
  buf[0] = (input >> 56) & 0xff;
  dst->assign(buf, sizeof(input));
}

inline void AssignIntToSlice(Slice* dst, uint64_t input) {
  dst->assign((char *)&input, IntLength(input));
}

inline uint64_t SliceToInt2 (const Slice& input) {
  uint64_t res = 0UL;
  memcpy (&res, input.data(), input.size());
  // res = *((uint64_t*) input.data());
  return res;
}

inline uint64_t SliceToInt (const Slice& input) {
  const char* ptr = input.data();
  uint64_t res = 0UL;
  int i;
  for (i = 0; i < input.size(); i++) {
    res <<= 8;
    res |= (static_cast<uint64_t>(static_cast<unsigned char>(ptr[i])));
  }
  return res;
  // return ((static_cast<uint64_t>(static_cast<unsigned char>(ptr[0])))
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[1])) << 8)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[2])) << 16)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[3])) << 24)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[4])) << 32)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[5])) << 40)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[6])) << 48)
  //       | (static_cast<uint64_t>(static_cast<unsigned char>(ptr[7])) << 56));

}

inline void IntToSlice (Slice& s, char* buf, uint64_t input) {
  buf[7] =  input & 0xff;
  buf[6] = (input >> 8) & 0xff;
  buf[5] = (input >> 16) & 0xff;
  buf[4] = (input >> 24) & 0xff;
  buf[3] = (input >> 32) & 0xff;
  buf[2] = (input >> 40) & 0xff;
  buf[1] = (input >> 48) & 0xff;
  buf[0] = (input >> 56) & 0xff;
  s.assign(buf, sizeof(input));
}

inline Slice IntToSlice2 (const uint64_t input) {
  // size_t size = IntLength(input);
  // char* buf = new char[size];
  // memcpy(buf, &input, size);
  // return Slice(buf, size);
  std::string* str = new std::string((char *)&input, IntLength(input));
  return Slice(*str);
}

inline Slice IntToSliceBuggy (const uint64_t input) {
  size_t size = IntLength(input);
  char buf[size];
  memcpy (buf, &input, size);
  return Slice(buf, size);
  // return Slice((char *) &input);
}


inline std::string* IntToString (const uint64_t input) {
  // size_t size = IntLength(input);
  // char* buf = new char[size];
  // memcpy(buf, &input, size);
  // return Slice(buf, size);
  return new std::string((char *)&input, IntLength(input));
}

inline std::string IntToStringBuggy (uint64_t input) {
  size_t size = IntLength(input);
  char* buf = new char[size];
  memcpy(buf, &input, size);
  // *buf = *(char *)&input;
  return std::string(buf, size);
}

inline uint64_t StringToInt (const std::string& input) {
  uint64_t res = 0;
  memcpy (&res, input.data(), input.size());
  // res = *((uint64_t*) input.data());
  return res;
}

inline uint64_t GetVarint(const Slice& input) {
  uint64_t result = 0;
  const char* p = input.data();
  size_t len = input.size();
  for (uint32_t shift = 0, i = 0; shift <= 56 && i < len; shift += 8, i++) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    printf("byte = %llu\n", (long long unsigned) byte);
    p++;

    result |= ((byte & 0xff) << shift);
  }
  printf("Done result = %llu\n", (long long unsigned) result);
  return result;
}

inline Slice PutVarint(uint64_t input) {
  char buf[10];
  unsigned char* p = reinterpret_cast<unsigned char*>(buf);
  while (input > 0) {
    unsigned char c = input & 0xff;
    // printf("%u\n", c);
    *(p++) = c;
    input >>= 8;
  }

  return Slice(buf, reinterpret_cast<char*>(p) - buf);
}
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CODING_H_
