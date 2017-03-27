#include "util/testharness.h"
#include "ssmem.h"
#include "utils.h"
#include "atomic_ops.h"
#include "rapl_read.h"
#include "latency.h"
#include "main_test_loop.h"
#include "common.h"
#include "clht_lb_res.h"
#include "util/coding.h"

namespace leveldb {

size_t kNumThreads = 1;

class CLHTTest { };

TEST(CLHTTest, AlwaysCorrect) {
    ASSERT_TRUE(true);
}

TEST(CLHTTest, CreateAndDestroy) {
    clht_t* ht = clht_create(1024);
    clht_gc_thread_init(ht, 0);
    clht_gc_destroy(ht);
}

TEST(CLHTTest, LargeCreateAndDestroy) {
    clht_t* ht = clht_create(2UL<<20);
    clht_gc_thread_init(ht, 0);
    clht_gc_destroy(ht);
}

TEST(CLHTTest, LargeKeySmallMemtable) {
    clht_t* ht = clht_create(128);
    clht_gc_thread_init(ht, 0);
  
    ASSERT_EQ(1, clht_put(ht, 1UL<<50, 1UL<<50));
    ASSERT_EQ(1UL<<50, clht_get(ht->ht, 1UL<<50));

    clht_gc_destroy(ht);
}

TEST(CLHTTest, SmallIterator) {
    clht_t* ht = clht_create(128);
    clht_gc_thread_init(ht, 0);

    for (uint64_t i = 1; i <= 64; i++) {
        ASSERT_EQ(1, clht_put(ht, i, i));
    }

    uint64_t count = 0;
    for (size_t i = 0; i < ht->ht->num_buckets; i++) {
        for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
            if (ht->ht->table[i].key[j] != 0) {
                count++;
            }
        }
    }

    ASSERT_EQ(64, count);

    clht_gc_destroy(ht);
}

TEST(CLHTTest, LargeIterator) {
    clht_t* ht = clht_create(128);
    clht_gc_thread_init(ht, 0);

    for (uint64_t i = 1; i <= 512; i++) {
        ASSERT_EQ(1, clht_put(ht, i, i));
    }

    uint64_t count = 0;
    for (size_t i = 0; i < ht->ht->num_buckets; i++) {
        for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
            if (ht->ht->table[i].key[j] != 0) {
                count++;
            }
        }
    }

    ASSERT_EQ(512, count);

    clht_gc_destroy(ht);
}

TEST(CLHTTest, PutDeleteGet) {
    clht_t* ht = clht_create(1024);
    clht_gc_thread_init(ht, 0);

    for (uint64_t i = 1; i <= 1000; i++) {
        ASSERT_EQ(1, clht_put(ht, i, i));
        ASSERT_EQ(i, clht_get(ht->ht, i));
    }

    for (uint64_t i = 1; i <= 1000; i++) {
        ASSERT_EQ(i, clht_remove(ht, i));
        ASSERT_EQ(0, clht_get(ht->ht, i));
    }

    clht_gc_destroy(ht);
}

TEST(CLHTTest, SlicePutDeleteGet) {
    clht_t* ht = clht_create(1024);
    clht_gc_thread_init(ht, 0);

    for (uint64_t i = 1; i <= 1000; i++) {

        ASSERT_EQ(1, clht_put(ht, SliceToInt(IntToSlice(i)), SliceToInt(IntToSlice(i))));
        ASSERT_EQ(SliceToInt(IntToSlice(i)), clht_get(ht->ht, SliceToInt(IntToSlice(i))));
    }

    for (uint64_t i = 1; i <= 1000; i++) {
        ASSERT_EQ(SliceToInt(IntToSlice(i)), clht_remove(ht, SliceToInt(IntToSlice(i))));
        ASSERT_EQ(0, clht_get(ht->ht, SliceToInt(IntToSlice(i))));
    }

    clht_gc_destroy(ht);
}

TEST(CLHTTest, DeleteIfSame) {
    clht_t* ht = clht_create(1024);
    clht_gc_thread_init(ht, 0);

    clht_put(ht, 1, 2);
    ASSERT_TRUE(clht_remove_if_same(ht, 1, 2));

    clht_put(ht, 2, 3);
    clht_put(ht, 2, 4);
    ASSERT_TRUE(!clht_remove_if_same(ht, 2, 3));

    clht_gc_destroy(ht);
}

} // namespace leveldb

int main(int argc, char** argv) {

  return leveldb::test::RunAllTests();
}