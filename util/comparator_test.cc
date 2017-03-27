// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/slice.h"
#include "util/coding.h"
#include "util/testharness.h"
#include <iostream>

namespace leveldb {

class ComparatorTest { };

TEST(ComparatorTest, AlwaysTrue) {
  ASSERT_TRUE(true);
}

TEST(ComparatorTest, IntToSlice) {
  Slice slice1, slice2;
  char* buf = new char[9];
  char* buf2 = new char[9];
  for (uint64_t i = 1; i <= 100000000; i++) {
    IntToSlice(slice1, buf, i);
    uint64_t x = SliceToInt(slice1);
    ASSERT_EQ(x, i);
    IntToSlice(slice2, buf2, x);
    ASSERT_EQ(0, slice1.compare2(slice2));
  }
}

TEST(ComparatorTest, Memcmp) {
  Slice igor("IGOR");
  Slice oana("OANA");
  Slice lol("LALTRO");

  uint64_t igor_int = SliceToInt(igor);
  uint64_t oana_int = SliceToInt(oana);
  uint64_t lol_int  = SliceToInt(lol);

  Slice slice;
  char* buf = new char[9];
  IntToSlice(slice, buf, igor_int);
  ASSERT_EQ(slice.compare2(igor), 0);
  std::cout << slice.ToString() << std::endl;

  memset(buf, 0, 8);
  IntToSlice(slice, buf, oana_int);  
  std::cout << slice.ToString() << std::endl;

  memset(buf, 0, 8);
  IntToSlice(slice, buf, lol_int);
  std::cout << slice.ToString() << std::endl;

  std::cout << "IGOR - OANA - LOL:" << igor_int << " " << oana_int << " " << lol_int << std::endl;

  std::cout << "FIRST COMPARATOR *****" << std::endl;

  std::cout << "IGOR < OANA? " << igor.compare(oana) << std::endl;
  std::cout << "IGOR < LOL? " << igor.compare(lol) << std::endl;
  std::cout << "OANA < LOL? " << oana.compare(lol) << std::endl;

  std::cout << "SECOND COMPARATOR *****" << std::endl;

  std::cout << "IGOR < OANA? " << igor.compare2(oana) << std::endl;
  std::cout << "IGOR < LOL? " << igor.compare2(lol) << std::endl;
  std::cout << "OANA < LOL? " << oana.compare2(lol) << std::endl;

}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
