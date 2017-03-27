#include "db/memtable.h"
#include "leveldb/comparator.h"
#include "util/testharness.h"
#include "ssmem.h"
#include "utils.h"
#include "atomic_ops.h"
#include "rapl_read.h"
#include "latency.h"
#include "main_test_loop.h"
#include "common.h"
#include <iostream>
#include "leveldb/write_batch.h"

extern __thread ssmem_allocator_t* alloc;
extern __thread unsigned long * seeds;
extern unsigned int levelmax;

namespace leveldb {

class MemtableTest { };

TEST(MemtableTest, AlwaysCorrect) {
    ASSERT_TRUE(true);
}

TEST(MemtableTest, Empty) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    Slice key("OANA");
    std::string val;
    Status s;
    LookupKey lkey(key, kMaxSequenceNumber);
    ASSERT_TRUE(!m->Get(lkey, &val, &s));
}

TEST(MemtableTest, Add) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    Slice key("OANA");
    std::string val;

    m->Add(0, kTypeValue, key, key);

    Status s;
    LookupKey lkey(key, kMaxSequenceNumber);
    ASSERT_TRUE(m->Get(lkey, &val, &s));
    ASSERT_TRUE(s.ok());
    ASSERT_EQ("OANA", val);
}

TEST(MemtableTest, AddIntToSlice) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    std::string val;
    Status s;

    for (uint64_t j=1; j < 512; j++) {
        Slice key = IntToSlice(j);
        m->Add(0, kTypeValue, key, key);
        LookupKey lkey(key, kMaxSequenceNumber);
        ASSERT_TRUE(m->Get(lkey, &val, &s));
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(j, SliceToInt(Slice(val)));
    }
}

TEST(MemtableTest, AddAdd) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    Slice oana("OANA");
    Slice igor("IGOR");
    std::string val;

    m->Add(0, kTypeValue, oana, oana);

    Status s;
    LookupKey lkey(oana, kMaxSequenceNumber);
    m->Get(lkey, &val, &s);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ("OANA", val);

    m->Add(0, kTypeValue, oana, igor);
    m->Get(lkey, &val, &s);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ("IGOR", val);
}

TEST(MemtableTest, AddDeleteAdd) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    Slice oana("OANA");
    Slice igor("IGOR");
    std::string val;

    m->Add(0, kTypeValue, oana, oana);

    Status s1, s2, s3;
    LookupKey lkey(oana, kMaxSequenceNumber);
    m->Get(lkey, &val, &s1);
    ASSERT_TRUE(s1.ok());
    ASSERT_EQ("OANA", val);

    m->Add(1, kTypeDeletion, oana, Slice());
    m->Get(lkey, &val, &s2);
    ASSERT_TRUE(s2.IsNotFound());

    m->Add(2, kTypeValue, oana, igor);
    m->Get(lkey, &val, &s3);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ("IGOR", val);
}

TEST(MemtableTest, EmptyIteratorInvalid) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);
    Iterator* iter = m->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
}

TEST(MemtableTest, NonEmptyIteratorInvalid) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);

    Slice oana("OANA");
    m->Add(0, kTypeValue, oana, oana);

    Iterator* iter = m->NewIterator();
    ASSERT_TRUE(!iter->Valid());

    delete iter;
}

TEST(MemtableTest, EmptyIteratorSeekToFirstInvalid) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);

    Iterator* iter = m->NewIterator();
    iter->SeekToFirst();
    ASSERT_TRUE(!iter->Valid());

    delete iter;
}

TEST(MemtableTest, NonEmptyIteratorSeekToFirstValid) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);

    Slice oana("OANA");
    m->Add(0, kTypeValue, oana, oana);

    Iterator* iter = m->NewIterator();
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());

    delete iter;
}

TEST(MemtableTest, IteratorReturnValue) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);

    Slice oana("OANA");
    m->Add(0, kTypeValue, oana, oana);

    Iterator* iter = m->NewIterator();
    iter->SeekToFirst();
    InternalKey k;
    k.DecodeFrom(iter->key()); 
    ASSERT_EQ("OANA", k.user_key().ToString());

    delete iter;
}

TEST(MemtableTest, IterateTest) {
    InternalKeyComparator cmp(BytewiseComparator());    
    MemTable* m = new MemTable(cmp);

    Slice oana("OANA");
    Slice igor("IGOR");
    Slice lol("LOL");
    m->Add(0, kTypeValue, oana, oana);
    m->Add(1, kTypeValue, igor, igor);
    m->Add(2, kTypeValue, lol, lol);


    Iterator* iter = m->NewIterator();
    iter->SeekToFirst();
    for ( ; iter->Valid(); iter->Next()) {
        InternalKey k;
        k.DecodeFrom(iter->key());
        std::cout << k.user_key().ToString() << std::endl;        
    }

#ifdef ASCY_MEMTABLE
    ASSERT_LT(cmp.user_comparator()->Compare(lol, oana), 0);
    ASSERT_LT(cmp.user_comparator()->Compare(lol, igor), 0);
    ASSERT_LT(cmp.user_comparator()->Compare(oana, igor), 0);
#endif

    delete iter;
}

TEST(MemtableTest, SkiplistDeleteTest) {
    uint64_t number_of_keys = 5<<22;
    uint64_t key;
    levelmax = floor_log_2(number_of_keys);

    sl_intset_t* set;
    set = sl_set_new();


    for (key = 0; key <= number_of_keys; key++) {
      sl_add(set, key, key, 0);
    }

    std::cout << "Freeing memory" << std::endl;
    sl_set_delete(set);
    std::cout << "Done Freeing memory" << std::endl;

    struct timespec timeout;
    timeout.tv_sec = 10;
    timeout.tv_nsec = 0;

    nanosleep(&timeout, NULL);

    set = sl_set_new();

    for (key = 0; key <= number_of_keys; key++) {
      sl_add(set, key, key, 0);
    }

    std::cout << "Freeing memory" << std::endl;
    sl_set_delete(set);
    std::cout << "Done Freeing memory" << std::endl;
}

TEST(MemtableTest, DestroyTest) {
    InternalKeyComparator cmp(BytewiseComparator());    
    uint64_t number_of_keys = 5<<22;
    uint64_t key;
    levelmax = floor_log_2(number_of_keys);

    MemTable* m = new MemTable(cmp);

    Slice key_slice;
    char* buf = new char[9];

    for (key = 0; key <= number_of_keys; key++) {
      memcpy(buf, &key, sizeof(key));
      key_slice.assign(buf, sizeof(key));
      m->Add(key, kTypeValue, key_slice, key_slice);
    }

    std::cout << "Freeing memory" << std::endl;
    m->Delete();
    std::cout << "Done Freeing memory" << std::endl;

    struct timespec timeout;
    timeout.tv_sec = 10;
    timeout.tv_nsec = 0;

    nanosleep(&timeout, NULL);

    m = new MemTable(cmp);

    for (key = 0; key <= number_of_keys; key++) {
      memcpy(buf, &key, sizeof(key));
      key_slice.assign(buf, sizeof(key));
      m->Add(key, kTypeValue, key_slice, key_slice);
    }

    std::cout << "Freeing memory" << std::endl;
    m->Delete();
    std::cout << "Done Freeing memory" << std::endl;
    delete[] buf;
}

TEST(MemtableTest, ThreeMallocTest) {
  void* ptr1 = malloc(1000);
  free(ptr1);
  void* ptr2 = ssmem_alloc(alloc, 1000);
  free(ptr2);
  void* ptr3 = (void *) new char[1000];
  delete[] ptr3;
}

TEST(MemtableTest, SsmemFreeTest) {
    uint64_t number_of_keys = 1000000;
    void ** ptrs = new void*[number_of_keys];
    uint64_t key;

    for (key = 0; key <= number_of_keys; key++) {
      ptrs[key] = ssmem_alloc(alloc,10000);
      *(uint64_t *)ptrs[key] = key;
    }

    std::cout << "Done" << std::endl;
    struct timespec timeout;
    timeout.tv_sec = 10;
    timeout.tv_nsec = 0;
    nanosleep(&timeout, NULL);

    for (key = 0; key <= number_of_keys; key++) {
      printf("%llu\n", *(uint64_t *)ptrs[key]);
      free(ptrs[key]);
    }

    std::cout << "Done freeing" << std::endl;
    nanosleep(&timeout, NULL);

    for (key = 0; key <= number_of_keys; key++) {
      ptrs[key] = ssmem_alloc(alloc, 10000);
      *(uint64_t *)ptrs[key] = key;
    }

    std::cout << "Done" << std::endl;
    nanosleep(&timeout, NULL);


    for (key = 0; key <= number_of_keys; key++) {
      printf("%llu\n", *(uint64_t *)ptrs[key]);
      free(ptrs[key]);
    }

    std::cout << "Done freeing" << std::endl;
    nanosleep(&timeout, NULL);

}

#ifdef ASCY_MEMTABLE
RETRY_STATS_VARS_GLOBAL;

size_t kNumThreads = 4;
size_t kTestSeconds = 10;
size_t kInitial = 1024;
size_t kRange = 2048;
size_t kUpdatePercent = 0;

size_t print_vals_num = 100;
size_t pf_vals_num = 1023;
size_t put, put_explicit = false;
double update_rate, put_rate, get_rate;

size_t size_after = 0;
int seed = 0;
uint64_t rand_max;
#define rand_min 1

static volatile int stop;
TEST_VARS_GLOBAL;

volatile ticks *putting_succ;
volatile ticks *putting_fail;
volatile ticks *getting_succ;
volatile ticks *getting_fail;
volatile ticks *removing_succ;
volatile ticks *removing_fail;
volatile ticks *putting_count;
volatile ticks *putting_count_succ;
volatile ticks *getting_count;
volatile ticks *getting_count_succ;
volatile ticks *removing_count;
volatile ticks *removing_count_succ;
volatile ticks *total;

#ifdef DEBUG
extern __thread uint32_t put_num_restarts;
extern __thread uint32_t put_num_failed_expand;
extern __thread uint32_t put_num_failed_on_new;
#endif

barrier_t barrier, barrier_global;

// struct IgorMTState {
//   DBTest* test;
//   port::AtomicPointer stop;
//   port::AtomicPointer counter[kNumThreads];
//   port::AtomicPointer thread_done[kNumThreads];
// };

struct IgorMTThread {
  // MTState* state;
  uint32_t id;
  MemTable* mem;
};

void* IgorMTThreadBody(void* arg) {

  IgorMTThread* t = reinterpret_cast<IgorMTThread*>(arg);
  uint32_t id = t->id;
  MemTable* mem = t->mem;
  int phys_id = the_cores[id];
  set_cpu(phys_id);
  ssalloc_init();

  PF_INIT(3, SSPFD_NUM_ENTRIES, id);

#if defined(COMPUTE_LATENCY)
  volatile ticks my_putting_succ = 0;
  volatile ticks my_putting_fail = 0;
  volatile ticks my_getting_succ = 0;
  volatile ticks my_getting_fail = 0;
  volatile ticks my_removing_succ = 0;
  volatile ticks my_removing_fail = 0;
#endif
  uint64_t my_putting_count = 0;
  uint64_t my_getting_count = 0;
  uint64_t my_removing_count = 0;

  uint64_t my_putting_count_succ = 0;
  uint64_t my_getting_count_succ = 0;
  uint64_t my_removing_count_succ = 0;

#if defined(COMPUTE_LATENCY) && PFD_TYPE == 0
  volatile ticks start_acq, end_acq;
  volatile ticks correction = getticks_correction_calc();
#endif

  seeds = seed_rand();
#if GC == 1
  alloc = (ssmem_allocator_t*) malloc(sizeof(ssmem_allocator_t));
  assert(alloc != NULL);
  ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, id);
#endif

  RR_INIT(phys_id);
  barrier_cross(&barrier);

  uint64_t key, c;
  
  std::string result;
  Status s;
  uint32_t scale_rem = (uint32_t) (update_rate * UINT_MAX);
  uint32_t scale_put = (uint32_t) (put_rate * UINT_MAX);
  printf("## Scale put: %zu / Scale rem: %zu\n", scale_put, scale_rem);

  uint32_t num_elems_thread = (uint32_t) (leveldb::kInitial / leveldb::kNumThreads);
  int32_t missing = (uint32_t) kInitial - (num_elems_thread * leveldb::kNumThreads);
  if (id < missing) {
      num_elems_thread++;
  }

#if INITIALIZE_FROM_ONE == 1
  num_elems_thread = (id == 0) * leveldb::kInitial;
#endif

#ifndef NO_INIT_FILL
#ifdef INIT_SEQ
  size_t j;
  if (id == 0) {
    for (j = leveldb::kRange - 1; j >= leveldb::kRange/2; j--) {
      if (j == 7*leveldb::kRange/8) {
        printf("inserting: 25/100 done, j = %zu \n", j);
      } else if (j == 3 * leveldb::kRange / 4) {
        printf("inserting: 50/100 done\n");
      } else if (j == 5 * leveldb::kRange / 8) {
        printf("inserting: 75/100 done\n");
      }
      mem->Add(0, kTypeValue, IntToSlice(j), IntToSlice(j));
    }
  }
#else
  int i;
  for(i = 0; i < num_elems_thread; i++) {
    key = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (rand_max + 1)) + rand_min;
    // snprintf(keybuf, sizeof(keybuf), "%d", key);
    // snprintf(valbuf, sizeof(valbuf), "%d", key);
    if (!mem->Get(LookupKey(IntToSlice(key), 0), &result, &s) || s.IsNotFound()) {
      mem->Add(0, kTypeValue, IntToSlice(key), IntToSlice(key));
    } else {
      i--;
    }
  }
#endif
#endif

  MEM_BARRIER;

  barrier_cross(&barrier);

  RETRY_STATS_ZERO();

  barrier_cross(&barrier_global);

  RR_START_SIMPLE();

  while (stop == 0) {
#ifdef SKEW9010
      TEST_LOOP_90_10(NULL);
#else
    c = (uint32_t)(my_random(&(seeds[0]),&(seeds[1]),&(seeds[2]))); 
    key = (c & rand_max) + rand_min;    
    // snprintf(keybuf, sizeof(keybuf), "%d", key);
    // snprintf(valbuf, sizeof(valbuf), "%d", key);      
                  
    if (unlikely(c <= scale_put)) {                 
        START_TS(1);              
        mem->Add(0, kTypeValue, IntToSlice(key), IntToSlice(key));
        if(s.ok()) {               
          END_TS(1, my_putting_count_succ);       
          ADD_DUR(my_putting_succ);         
          my_putting_count_succ++;          
        }               
      END_TS_ELSE(4, my_putting_count - my_putting_count_succ,    
      my_putting_fail);         
      my_putting_count++;           
    } else if(unlikely(c <= scale_rem)) {
      START_TS(2);              
      mem->Add(0, kTypeDeletion, IntToSlice(key), Slice());
      if(s.ok()) {               
        END_TS(2, my_removing_count_succ);        
        ADD_DUR(my_removing_succ);          
        my_removing_count_succ++;         
      }               
      END_TS_ELSE(5, my_removing_count - my_removing_count_succ, my_removing_fail);          
      my_removing_count++;            
    } else {                 
      START_TS(0);              
      if (mem->Get(LookupKey(IntToSlice(key), 0), &result, &s) && s.ok()) {
        END_TS(0, my_getting_count_succ);       
        ADD_DUR(my_getting_succ);         
        my_getting_count_succ++;          
      }               
      END_TS_ELSE(3, my_getting_count - my_getting_count_succ,    
      my_getting_fail);         
      my_getting_count++;           
    }
#endif
  }

  barrier_cross(&barrier);
  RR_STOP_SIMPLE();

  barrier_cross(&barrier);

#if defined(COMPUTE_LATENCY)
  putting_succ[id] += my_putting_succ;
  putting_fail[id] += my_putting_fail;
  getting_succ[id] += my_getting_succ;
  getting_fail[id] += my_getting_fail;
  removing_succ[id] += my_removing_succ;
  removing_fail[id] += my_removing_fail;
#endif
  putting_count[id] += my_putting_count;
  getting_count[id] += my_getting_count;
  removing_count[id]+= my_removing_count;

  putting_count_succ[id] += my_putting_count_succ;
  getting_count_succ[id] += my_getting_count_succ;
  removing_count_succ[id]+= my_removing_count_succ;

  EXEC_IN_DEC_ID_ORDER(id, leveldb::kNumThreads)
    {
      print_latency_stats(id, SSPFD_NUM_ENTRIES, print_vals_num);
      RETRY_STATS_SHARE();
    }
  EXEC_IN_DEC_ID_ORDER_END(&barrier);

  SSPFDTERM();
#if GC == 1
  ssmem_term();
  free(alloc);
#endif

  pthread_exit(NULL);
}

// }  // namespace

TEST(MemtableTest, IgorMultiThreaded) {

  set_cpu(the_cores[0]);

  struct timeval start, end;
  struct timespec timeout;
  timeout.tv_sec = leveldb::kTestSeconds / 1000;
  timeout.tv_nsec = (leveldb::kTestSeconds % 1000) * 1000000;

  stop = 0;

  /* Initializes the local data */
  putting_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  putting_fail = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  getting_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  getting_fail = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  removing_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  removing_fail = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  putting_count = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  putting_count_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  getting_count = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  getting_count_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  removing_count = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));
  removing_count_succ = (ticks *) calloc(leveldb::kNumThreads , sizeof(ticks));

  pthread_t threads[leveldb::kNumThreads];
  pthread_attr_t attr;
  int rc;
  void *status;

  barrier_init(&barrier_global, leveldb::kNumThreads + 1);
  barrier_init(&barrier, leveldb::kNumThreads);

  /* Initialize and set thread detached attribute */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  IgorMTThread* tds = (IgorMTThread*) malloc(leveldb::kNumThreads * sizeof(IgorMTThread));

  InternalKeyComparator cmp(BytewiseComparator());    
  MemTable* m = new MemTable(cmp);

  long t;
  for(t = 0; t < leveldb::kNumThreads; t++) {
    tds[t].id = t;
    tds[t].mem = m;
    rc = pthread_create(&threads[t], &attr, IgorMTThreadBody, tds + t);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }

  /* Free attribute and wait for the other threads */
  pthread_attr_destroy(&attr);

  barrier_cross(&barrier_global);
  gettimeofday(&start, NULL);
  nanosleep(&timeout, NULL);

  stop = 1;
  gettimeofday(&end, NULL);
  leveldb::kTestSeconds = (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000);

  for(t = 0; t < leveldb::kNumThreads; t++) {
    rc = pthread_join(threads[t], &status);
    if (rc) {
      printf("ERROR; return code from pthread_join() is %d\n", rc);
      exit(-1);
    }
  }

  free(tds);

  volatile ticks putting_suc_total = 0;
  volatile ticks putting_fal_total = 0;
  volatile ticks getting_suc_total = 0;
  volatile ticks getting_fal_total = 0;
  volatile ticks removing_suc_total = 0;
  volatile ticks removing_fal_total = 0;
  volatile uint64_t putting_count_total = 0;
  volatile uint64_t putting_count_total_succ = 0;
  volatile uint64_t getting_count_total = 0;
  volatile uint64_t getting_count_total_succ = 0;
  volatile uint64_t removing_count_total = 0;
  volatile uint64_t removing_count_total_succ = 0;

  for(t=0; t < leveldb::kNumThreads; t++) {
    putting_suc_total += putting_succ[t];
    putting_fal_total += putting_fail[t];
    getting_suc_total += getting_succ[t];
    getting_fal_total += getting_fail[t];
    removing_suc_total += removing_succ[t];
    removing_fal_total += removing_fail[t];
    putting_count_total += putting_count[t];
    putting_count_total_succ += putting_count_succ[t];
    getting_count_total += getting_count[t];
    getting_count_total_succ += getting_count_succ[t];
    removing_count_total += removing_count[t];
    removing_count_total_succ += removing_count_succ[t];
  }

#if defined(COMPUTE_LATENCY)
  printf("#thread srch_suc srch_fal insr_suc insr_fal remv_suc remv_fal   ## latency (in cycles) \n"); fflush(stdout);
  long unsigned get_suc = (getting_count_total_succ) ? getting_suc_total / getting_count_total_succ : 0;
  long unsigned get_fal = (getting_count_total - getting_count_total_succ) ? getting_fal_total / (getting_count_total - getting_count_total_succ) : 0;
  long unsigned put_suc = putting_count_total_succ ? putting_suc_total / putting_count_total_succ : 0;
  long unsigned put_fal = (putting_count_total - putting_count_total_succ) ? putting_fal_total / (putting_count_total - putting_count_total_succ) : 0;
  long unsigned rem_suc = removing_count_total_succ ? removing_suc_total / removing_count_total_succ : 0;
  long unsigned rem_fal = (removing_count_total - removing_count_total_succ) ? removing_fal_total / (removing_count_total - removing_count_total_succ) : 0;
  printf("%-7zu %-8lu %-8lu %-8lu %-8lu %-8lu %-8lu\n", leveldb::kNumThreads, get_suc, get_fal, put_suc, put_fal, rem_suc, rem_fal);
#endif

#define LLU long long unsigned int

  // int UNUSED pr = (int) (putting_count_total_succ - removing_count_total_succ);
  // if (size_after != (leveldb::kInitial + pr)) {
  //   printf("// WRONG size. %zu + %d != %zu\n", leveldb::kInitial, pr, size_after);
  //   assert(size_after == (leveldb::kInitial + pr));
  // }

  printf("    : %-10s | %-10s | %-11s | %-11s | %s\n", "total", "success", "succ %", "total %", "effective %");
  uint64_t total = putting_count_total + getting_count_total + removing_count_total;
  double putting_perc = 100.0 * (1 - ((double)(total - putting_count_total) / total));
  double putting_perc_succ = (1 - (double) (putting_count_total - putting_count_total_succ) / putting_count_total) * 100;
  double getting_perc = 100.0 * (1 - ((double)(total - getting_count_total) / total));
  double getting_perc_succ = (1 - (double) (getting_count_total - getting_count_total_succ) / getting_count_total) * 100;
  double removing_perc = 100.0 * (1 - ((double)(total - removing_count_total) / total));
  double removing_perc_succ = (1 - (double) (removing_count_total - removing_count_total_succ) / removing_count_total) * 100;
  printf("srch: %-10llu | %-10llu | %10.1f%% | %10.1f%% | \n", (LLU) getting_count_total,
   (LLU) getting_count_total_succ,  getting_perc_succ, getting_perc);
  printf("insr: %-10llu | %-10llu | %10.1f%% | %10.1f%% | %10.1f%%\n", (LLU) putting_count_total,
   (LLU) putting_count_total_succ, putting_perc_succ, putting_perc, (putting_perc * putting_perc_succ) / 100);
  printf("rems: %-10llu | %-10llu | %10.1f%% | %10.1f%% | %10.1f%%\n", (LLU) removing_count_total,
   (LLU) removing_count_total_succ, removing_perc_succ, removing_perc, (removing_perc * removing_perc_succ) / 100);

  double throughput = (putting_count_total + getting_count_total + removing_count_total) * 1000.0 / leveldb::kTestSeconds;
  printf("#txs %zu\t(%-10.0f\n", leveldb::kNumThreads, throughput);
  printf("#Mops %.3f\n", throughput / 1e6);

  RR_PRINT_UNPROTECTED(RAPL_PRINT_POW);
  RR_PRINT_CORRECTED();
  RETRY_STATS_PRINT(total, putting_count_total, removing_count_total, putting_count_total_succ + removing_count_total_succ);

  pthread_exit(NULL);
}
#endif

} // namespace leveldb

int main(int argc, char** argv) {

#ifdef ASCY_MEMTABLE
  setenv("LEVELDB_TESTS", "DestroyTest", true);
  // setenv("TEST_TMPDIR", "/storage/leveldbtest/", true);

  struct option long_options[] = {
    // These options don't set a flag
    {"help",                      no_argument,       NULL, 'h'},
    {"duration",                  required_argument, NULL, 'd'},
    {"initial-size",              required_argument, NULL, 'i'},
    {"num-threads",               required_argument, NULL, 'n'},
    {"range",                     required_argument, NULL, 'r'},
    {"update-rate",               required_argument, NULL, 'u'},
    {"num-buckets",               required_argument, NULL, 'b'},
    {"print-vals",                required_argument, NULL, 'v'},
    {"vals-pf",                   required_argument, NULL, 'f'},
    {NULL, 0, NULL, 0}
  };

  int i, c;
  while(1) {
    i = 0;
    c = getopt_long(argc, argv, "hAf:d:i:n:r:s:u:m:a:l:p:b:v:f:e:", long_options, &i);

    if(c == -1)
      break;

    if(c == 0 && long_options[i].flag == 0)
      c = long_options[i].val;

    switch(c) {
      case 0:
        /* Flag is automatically set */
        break;
      case 'h':
        printf("ASCYLIB -- stress test "
         "\n"
         "\n"
         "Usage:\n"
         "  %s [options...]\n"
         "\n"
         "Options:\n"
         "  -h, --help\n"
         "        Print this message\n"
         "  -d, --duration <int>\n"
         "        Test duration in milliseconds\n"
         "  -i, --initial-size <int>\n"
         "        Number of elements to insert before test\n"
         "  -n, --num-threads <int>\n"
         "        Number of threads\n"
         "  -r, --range <int>\n"
         "        Range of integer values inserted in set\n"
         "  -u, --update-rate <int>\n"
         "        Percentage of update transactions\n"
         , argv[0]);
        exit(0);
      case 'd':
        leveldb::kTestSeconds = atoi(optarg);
        break;
      case 'i':
        leveldb::kInitial = atol(optarg);
        break;
      case 'n':
        leveldb::kNumThreads = atoi(optarg);
        break;
      case 'r':
        leveldb::kRange = atol(optarg);
        break;
      case 'u':
        leveldb::kUpdatePercent = atoi(optarg);
        break;
      case '?':
      default:
        printf("Use -h or --help for help\n");
        exit(1);
    }
  }

  if (!is_power_of_two(leveldb::kInitial)) {
      size_t initial_pow2 = pow2roundup(leveldb::kInitial);
      printf("** rounding up initial (to make it power of 2): old: %zu / new: %zu\n", leveldb::kInitial, initial_pow2);
      leveldb::kInitial = initial_pow2;
  }

  if (leveldb::kRange < leveldb::kInitial) {
      leveldb::kRange = 2 * leveldb::kInitial;
  }

  if (!is_power_of_two(leveldb::kRange)) {
      size_t range_pow2 = pow2roundup(leveldb::kRange);
      printf("** rounding up range (to make it power of 2): old: %zu / new: %zu\n", leveldb::kRange, range_pow2);
      leveldb::kRange = range_pow2;
  }

  leveldb::update_rate = leveldb::kUpdatePercent / 100.0;
  leveldb::put_rate = leveldb::update_rate / 2.0;
  printf("## Update rate: %f / Put rate: %f\n", leveldb::update_rate, leveldb::put_rate);

  printf("## Initial: %zu / Range: %zu\n", leveldb::kInitial, leveldb::kRange);
  levelmax = floor_log_2(leveldb::kInitial);
  printf("Levelmax = %d\n", levelmax);

  ssalloc_init();
  seeds = seed_rand();
  leveldb::rand_max = leveldb::kRange - 1;


  // // ASSERT_OK(PutNoWAL("Igor", ""));
#if GC == 1
  alloc = (ssmem_allocator_t*) malloc(sizeof(ssmem_allocator_t));
  assert(alloc != NULL);
  ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, 0);
#endif

#if defined(TIGHT_ALLOC)
  int level;
  size_t total_bytes = 0;
  size_t ns;
  for (level = 1; level <= levelmax; ++level) {
    // TODO remove magic number below
    // 32 = sizeof (key + val + 2*uint32 + sl_node_t*)
    ns = 32 + level * sizeof(void *);
    // if (ns % 32 != 0) {
    //   ns = 32 * (ns/32 + 1);
    // }
    // switch (log_base) {
    //   case 2:
        printf("nodes at level %d : %d\n", level, (leveldb::kInitial >> level));
        total_bytes += ns * (leveldb::kInitial >> level);
        // break;
    //   case 4:
    //     printf("nodes at level %d : %d\n", level, 3 * (initial >> (2 * level)));
    //     total_bytes += ns * 3 * (initial >> (2 * level));
    //     break;
    //   case 8:
    //     printf("nodes at level %d : %d\n", level, 7 * (initial >> (3 * level)));
    //     total_bytes += ns * 7 * (initial >> (3 * level));
    //     break;
    // }
  }
  double kb = total_bytes/1024.0; 
  double mb = kb / 1024.0;
  printf("Sizeof initial: %.2f KB = %.2f MB = %.2f GB\n", kb, mb, mb / 1024);
#endif

#endif // ASCY_MEMTABLE
  
  return leveldb::test::RunAllTests();

}
