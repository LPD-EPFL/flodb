#include "util/testharness.h"
#include "util/thread_local.h"
#include "leveldb/env.h"
#include "port/atomic_pointer.h"

namespace leveldb {

namespace {
void DelayMilliseconds(int millis) {
  Env::Default()->SleepForMicroseconds(millis * 1000);
}
} // namespace

class RCUTest {};

TEST(RCUTest, AlwaysTrue) {
    ASSERT_TRUE(true);
}

// Multi-threaded test:
namespace {

static const int kNumThreads = 7;
static const int kTestSeconds = 2;

uint64_t ALIGNED(CACHE_LINE_SIZE) x;
uint64_t ALIGNED(CACHE_LINE_SIZE) y;
uint64_t* ALIGNED(CACHE_LINE_SIZE) p; 

struct MTState {
  RCUTest* test;
  port::AtomicPointer stop;
  port::AtomicPointer thread_done[kNumThreads];
};

struct MTThread {
  MTState* state;
  int id;
};

static void MTThreadBody(void* arg) {
  MTThread* t = reinterpret_cast<MTThread*>(arg);
  int id = t->id;

  fprintf(stderr, "... starting thread %d\n", id);
  while (t->state->stop.Acquire_Load() == NULL) {
    rcu_function_start();

    ASSERT_NE(*p, 0);

    rcu_function_end();
  }
  t->state->thread_done[id].Release_Store(t);
  fprintf(stderr, "... stopping thread %d\n", id);
}

}  // namespace

TEST(RCUTest, MultiThreaded) {
    // Initialize state
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
      mt.thread_done[id].Release_Store(0);

    }
    
    x = 2;
    y = 3;
    p = &x;

    // Start threads
    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
      thread[id].state = &mt;
      thread[id].id = id;
      Env::Default()->StartThread(MTThreadBody, &thread[id]);
    }

    // Let them run for a while
    DelayMilliseconds((kTestSeconds * 1000)/2);
    p = &y;
    rcu_wait_others();
    x = 0;
    DelayMilliseconds((kTestSeconds * 1000)/2);

    // Stop the threads and wait for them to finish
    mt.stop.Release_Store(&mt);
    for (int id = 0; id < kNumThreads; id++) {
      while (mt.thread_done[id].Acquire_Load() == NULL) {
        DelayMilliseconds(100);
      }
    }
}


TEST(RCUTest, MultiThreadedThatFails) {
    // Initialize state
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
      mt.thread_done[id].Release_Store(0);

    }
    
    x = 2;
    y = 3;
    p = &x;

    // Start threads
    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
      thread[id].state = &mt;
      thread[id].id = id;
      Env::Default()->StartThread(MTThreadBody, &thread[id]);
    }

    // Let them run for a while
    DelayMilliseconds((kTestSeconds * 1000)/2);
    p = &y;
    x = 0;
    DelayMilliseconds((kTestSeconds * 1000)/2);

    // Stop the threads and wait for them to finish
    mt.stop.Release_Store(&mt);
    for (int id = 0; id < kNumThreads; id++) {
      while (mt.thread_done[id].Acquire_Load() == NULL) {
        DelayMilliseconds(100);
      }
    }
}

} // namespace leveldb


int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}