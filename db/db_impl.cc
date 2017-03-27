// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <iostream>
#include <map>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/membuffer.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"
#include <sys/time.h>
#include "leveldb/stats.h"

namespace leveldb {
extern size_t kNumThreads;
//uint64_t scan_cdf_array[11] = {0,0,0,0,0,0,0,0,0,0,0};

uint64_t nodes_written = 0;
double write_delay = 0; //write delay in microseconds
double write_delay2 = 0; //write delay in microseconds
int master_scanners = 0;
int piggy = 0;
int resets = 0;
uint64_t num_bg_thds = 1;

thread_local MemBufferIterator* membuf_iter;

const int kNumNonTableCacheFiles = 10;
const int kMaxNumScans = 10000;


// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      write_delay_mutex_(),
      write_delay_cv_(&write_delay_mutex_),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      stop_bg_work_(false),
      stop_disk_compactor_(true),
      manual_compaction_(NULL) {

  mem_ = new MemTable(internal_comparator_);
  mem_->Ref();
  has_imm_.Release_Store(NULL);
  membuf_ = new MemBuffer(options_.ht_threshold);
  immbuf_ = NULL;
  has_immbuf_.Release_Store(NULL);
  pause_membuffer_bg_thread_ = false;
  sl_write_flag = true;
  drain_helpers_count = 0;
  drain_done_count = 0;
  fallback_scanners = 0;

  scan_info.num_avail_scans = kMaxNumScans;
  scan_info.num_scans = 0;
  scan_info.seq_num = UINT_MAX;
  MEM_BARRIER;
  // write_delay_mutex_ = new port::Mutex();
  // write_delay_cv_(&write_delay_mutex_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
#ifdef ASCY_MEMTABLE
  env_->StartThread(&DBImpl::BGWork, this);
#ifdef DRAINING_THREADS
  for (int i = 0; i < DRAINING_THREADS; i++) {

    env_->StartThread(&DBImpl::BGWork2, this);
    
  }
#endif // DRAINING_THREADS

#ifndef NO_INIT_FILL
  env_->StartThread(&DBImpl::BGWork3, this);
#endif // NO_INIT_FILL
#endif // ASCY_MEMTABLE
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  stop_bg_work_ = true;
  mutex_.Lock();
  printf("Shutting down\n");
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }

  printf("Master scans: %d Piggy: %d Resets: %d\n", master_scanners, piggy, resets);
// #ifdef ASCY_MEMTABLE
  exit(0);
// #endif
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::ThreadInitDB(uint32_t id) {
  membuf_iter = (MemBufferIterator*) membuf_->NewIterator();
  membuf_iter->RandomEntry();
  membuf_->MemBufferThreadInit(id);
  table_cache_->TableCacheInitThread(id);
  THREAD_INIT_STATS_VARS
}


// returns true if it managed to move an entry (if the entry it landed on was valid)
bool DBImpl::MoveEntryToMemtable() {
#ifdef ASCY_MEMTABLE
  // membuf_rcu_function_start();
  uint64_t key, val;
  // membuf_iter->RandomEntry();
  membuf_iter->Next();

  if (membuf_iter->MarkAndGet(&key, &val)) {
    uint64_t seq = versions_->FetchAndIncrementLastSequence();

    mem_rcu_function_start();
    if (val == TOMBSTONE_VALUE) {
      //mem_->Add(seq, kTypeDeletion, key, get_unmarked_ref(val));
      mem_->Add(seq, kTypeDeletion, key, val);
    } else {
      //mem_->Add(seq, kTypeValue, key, get_unmarked_ref(val));
      mem_->Add(seq, kTypeValue, key, val);
    }
    mem_rcu_function_end();

    RECORD_TICK(entries_moved)
    membuf_->RemoveIfSameValue(key, get_marked_ref(val));
    // membuf_rcu_function_end();
    return true;
  }
  // membuf_rcu_function_end();
  return false;
#else
  return false;
#endif
}

#ifdef ASCY_MEMTABLE
void DBImpl::DrainImmutableMembuffer() {
  MemBufferIterator* iter = (MemBufferIterator*) immbuf_->NewIterator();
  size_t start_bucket = iter->RandomBucket();
  uint64_t keys[3];
  uint64_t vals[3];
  uint64_t seqs[3];

  do {

    // mark and get
    int nkeys = iter->MarkAndGetBucket(keys, vals);

    // drain
    if (nkeys > 0){
      int i;
      ValueType type;
      for (i = 0; i < nkeys; i++){
        seqs[i] = versions_->FetchAndIncrementLastSequence();
        
        if (keys[i] != 0 && vals[i] == 0) {
          printf("val == 0, key != 0: %lu, %lu\n", keys[i], vals[i]);
        }

        if (vals[i] == TOMBSTONE_VALUE) {
          type = kTypeDeletion;
        } else {
          type = kTypeValue;
        }

        RECORD_TICK(entries_moved)
        // mem_rcu_function_start();
        // mem_->Add(seqs[i], type, keys[i], vals[i]);
        // mem_rcu_function_end();
        seqs[i] = PackSequenceAndType(seqs[i], type);
      }

      mem_rcu_function_start();
#if ASCY_MEMTABLE == 3
      mem_->MultiInsert(keys, vals, seqs, nkeys);
#endif
      mem_rcu_function_end();

      RECORD_TICK(multi_inserts)

    }
    // increment bucket
    iter->AdvanceBucket();
  } while ((iter->WhichBucket() != start_bucket) && (drain_done_count == 0));

  // printf("Reached end of membuffer\n");
  delete iter;
}
#endif

#if ASCY_MEMTABLE == 3
// returns true if it managed to move an entry (if the entry it landed on was valid)
bool DBImpl::MoveBucketToMemtable() {

//TODO remove hardcoded value; 
//TODO figure out how to pick number of keys in multi insert

  // membuf_rcu_function_start();  
  uint64_t keys[3] = {0, 0, 0};
  uint64_t vals[3] = {0, 0, 0};
  uint64_t seqs[3] = {0, 0, 0};
  membuf_iter->NextBucket();

  int nkeys = membuf_iter->MarkAndGetBucket(keys, vals);
  // TODO fetch and increment sequence with nkeys
  // uint64_t seq = versions_->FetchAndIncrementLastSequence();

  if (nkeys > 0){
    int i;
    ValueType type;
    for (i = 0; i < nkeys; i++){
      seqs[i] = versions_->FetchAndIncrementLastSequence();
      
      if (keys[i] != 0 && vals[i] == 0) {
        printf("val == 0, key != 0: %lu, %lu\n", keys[i], vals[i]);
      }

      if (vals[i] == TOMBSTONE_VALUE) {
        type = kTypeDeletion;
      } else {
        type = kTypeValue;
      }

      RECORD_TICK(entries_moved)
      // mem_rcu_function_start();
      // mem_->Add(seqs[i], type, keys[i], vals[i]);
      // mem_rcu_function_end();
      seqs[i] = PackSequenceAndType(seqs[i], type);
    }

    mem_rcu_function_start();
    mem_->MultiInsert(keys, vals, seqs, nkeys);
    mem_rcu_function_end();

    RECORD_TICK(multi_inserts)
    membuf_iter->RemoveIfSameValuesBucket(keys, vals, nkeys);
    // membuf_rcu_function_end();
    return true;   
  } else {
    // membuf_rcu_function_end();
    return false;
  }
}

#endif


void DBImpl::BackgroundWork() {
#ifdef ASCY_MEMTABLE
  ssalloc_init();
  seeds = seed_rand();
#if GC == 1
  alloc = (ssmem_allocator_t*) malloc(sizeof(ssmem_allocator_t));
  assert(alloc != NULL);
  ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, 0);
#endif

  ThreadInitDB(0);

  bool compacted_last_time = false;
  uint64_t start, end;
  while(!stop_bg_work_) {
    if (!compacted_last_time) {
      env_->SleepForMicroseconds(options_.bg_thread_sleep_micros_max);
    }

    // 1. If memtable is full, prepare immutable for compaction
    // read thread stats
    int64_t sl_bytes = sl_total_written_bytes();
    // if (compacted_last_time) {
    //   std::cout << " Background thread no sleep, read " << sl_bytes << " bytes!" << std::endl;
    // } else {
    //   std::cout << " Background thread woke up, read " << sl_bytes << " bytes!" << std::endl;
    // }

    // std::cout << "Background thread woke up, ht size is " << ht_total_size() << std::endl;

    // if memtable full, do compaction
    if (sl_bytes > 0.95 * options_.skiplist_size) {


      // Attempt to switch to a new memtable and trigger compaction of old
      Status s;
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();

      // reset all bytes
      sl_reset_all_bytes();
      // wait for others
      mem_rcu_wait_others();
      compacted_last_time = true;
    } else {
      compacted_last_time = false;
    }

    // 2. Run compaction if necessary

    // if (imm_ != NULL || 
    //     manual_compaction_ != NULL || 
    //     versions_->NeedsCompaction()) {
    
    // Changed condition, only do memtable compactions
    if (imm_ != NULL) {
      if (shutting_down_.Acquire_Load() != NULL) {
        return;
      }
      Log(options_.info_log, "Going to compact %.1f MB\n", sl_bytes/(1024.0 * 1024.0));

      mutex_.Lock();
      bg_compaction_scheduled_ = true;
      start = env_->NowMicros();
      BackgroundCompaction();
      end = env_->NowMicros();
      bg_compaction_scheduled_ = false;
      bg_cv_.SignalAll();
      // write_delay_cv_.SignalAll();
      mutex_.Unlock();

      double diff_time = (end - start)/1000000.0;
      double node_throughput = nodes_written/diff_time;
      double byte_throughput = sl_bytes/(1024 * 1024 * diff_time);
      Log(options_.info_log, "Took %.1f seconds to compact %lu nodes. Throughput: %.1f nodes/s = %.1f", diff_time, nodes_written, node_throughput, byte_throughput);
      nodes_written = 0;
    } else {
      compacted_last_time = false;
    }

  }
#endif
}

void DBImpl::MembufferBGThread() {
#if defined(ASCY_MEMTABLE) && defined(USE_HT) 
  ssalloc_init();
  seeds = seed_rand();

  // // ASSERT_OK(PutNoWAL("Igor", ""));
#if GC == 1
  alloc = (ssmem_allocator_t*) malloc(sizeof(ssmem_allocator_t));
  assert(alloc != NULL);
  uint64_t id = FAI_U64(&num_bg_thds);
  id += kNumThreads;
  ssmem_alloc_init_fs_size(alloc, SSMEM_DEFAULT_MEM_SIZE, SSMEM_GC_FREE_SET_SIZE, id);
#endif // GC == 1

  ThreadInitDB(id);
  THREAD_INIT_STATS_VARS

  uint64_t iterations = 0;

//   while(!stop_bg_work_) {

// #if ASCY_MEMTABLE == 1
//     MoveEntryToMemtable();
// #elif ASCY_MEMTABLE == 3 
//     MoveBucketToMemtable();
// #endif  // ASCY_MEMTABLE == 1||2    
    
//     if (++iterations > 1000) {
//       COLLECT_THREAD_STATS
//       THREAD_INIT_STATS_VARS
//       iterations = 0;
//     }
//   }

  while(true) {
    if (stop_bg_work_) {
      return;
    }

    while(pause_membuffer_bg_thread_) {
      env_->SleepForMicroseconds(1000);
    }

    while (sl_total_written_bytes() >= 0.99 * options_.skiplist_size) {
      env_->SleepForMicroseconds(10);
    }
#if ASCY_MEMTABLE == 1
    MoveEntryToMemtable();
#elif ASCY_MEMTABLE == 3 
    MoveBucketToMemtable();
#endif  // ASCY_MEMTABLE == 1||2    
    
    if (++iterations > 1000) {
      COLLECT_THREAD_STATS
      THREAD_INIT_STATS_VARS
      // printf("Membuffer size: %d in %zu buckets\n", membuf_->size(), membuf_->NumberOfBuckets());
      iterations = 0;
    }

    // env_->SleepForMicroseconds(1000);
  }
#endif // ASCY_MEMTABLE && USE_HT
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          keep = true;
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

void FreeImmMemory(void * imm) {
  reinterpret_cast<MemTable*>(imm)->Delete();
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);

    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // printf("Compacting\n");
  // Save the contents of the memtable as a new Table
  Status s;
#ifndef DUMMY_PERSIST
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }
#endif // DUMMY_PERSIST

  if (s.ok()) {
    // Commit to the new state
    has_imm_.Release_Store(NULL);
#ifdef ASCY_MEMTABLE
    imm_rcu_wait_others();
    // imm_->Delete();
#ifndef DUMMY_PERSIST
    env_->StartThread(&FreeImmMemory, imm_);
#endif // DUMMY_PERSIST
#else 
    imm_->Unref();
#endif
    imm_ = NULL;
    // DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) { 
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
#ifdef ASCY_MEMTABLE
  reinterpret_cast<DBImpl*>(db)->BackgroundWork();
#else
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
#endif
}

void DBImpl::BGWork2(void *db) {
  reinterpret_cast<DBImpl*>(db)->MembufferBGThread();
}

void DBImpl::BGWork3(void *db) {
  reinterpret_cast<DBImpl*>(db)->DiskCompactor();
}

void DBImpl::DiskCompactor() {

  seeds = seed_rand();
  uint64_t start, end;
  while(true) {   

    env_->SleepForMicroseconds(1000);
    if (!stop_disk_compactor_) {
      // uint64_t start = env_->NowMicros();
      mutex_.Lock();
      if (!bg_error_.ok()) {
      } else if (versions_->NeedsCompaction()) {
          start = env_->NowMicros();
          BackgroundCompaction(false);
          end = env_->NowMicros();
          // printf("Compaction took %.1f seconds\n", (end-start)/1000000.0);
      }
      mutex_.Unlock();
      // uint64_t end = env_->NowMicros();
    }
  }
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction(bool check_compaction) {
  mutex_.AssertHeld();

  if (check_compaction && imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
    std::cout << "nothing to do" << std::endl;
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    // std::cout << "is trivial move" << std::endl;

    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    // if (has_imm_.NoBarrier_Load() != NULL) {
    //   const uint64_t imm_start = env_->NowMicros();
    //   mutex_.Lock();
    //   if (imm_ != NULL) {
    //     CompactMemTable();
    //     bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
    //   }
    //   mutex_.Unlock();
    //   imm_micros += (env_->NowMicros() - imm_start);
    // }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::RangeScan(const ReadOptions& options, const Slice& low, const Slice& high, std::map<Slice, Slice, bool(*)(Slice,Slice)>* result) {
  ScanInfo local_scan_info;
  bool i_am_in_fallback = false;
  uint64_t this_scan_restarts = 0;
while (true) {
    local_scan_info = scan_info;
    if (local_scan_info.num_avail_scans == 0) {
      env_->SleepForMicroseconds(10);
    } else {
      ScanInfo new_scan_info;
      new_scan_info.num_avail_scans = local_scan_info.num_avail_scans - 1;
      new_scan_info.num_scans = local_scan_info.num_scans + 1;
      new_scan_info.seq_num = local_scan_info.seq_num;
      if (CAS_U64_bool(&(scan_info.comparable), local_scan_info.comparable, new_scan_info.comparable)) {
        if (new_scan_info.num_scans == 1) { // I am the master scanner
          master_scanners++;
          uint32_t seq = scan_info.seq_num;
          bool res;

          // res = MasterScan(low, high, result);

          if (seq == UINT_MAX) {
            res = MasterScan(low, high, result);
          } else {
            res = ActualScan(low, high, seq, result);
          }
          ScanExitProcedure();
          if (res) {
            // FALLBACK
            if (i_am_in_fallback) {
              i_am_in_fallback=false;
              FAD_U64(&fallback_scanners);
            }

            if (this_scan_restarts < 10){
                RECORD_TICK(scan_cdf_array[this_scan_restarts]);          
            } else {
                RECORD_TICK(scan_cdf_array[10]);
            } 
            return Status();
          } else {
            // return Status::NotFound(Slice());

            // FALLBACK
            this_scan_restarts++;
            if (this_scan_restarts > 10 && !i_am_in_fallback) {
              i_am_in_fallback = true;
              RECORD_TICK(scan_fallbacks);
              FAI_U64(&fallback_scanners);
            }
            RECORD_TICK(scan_restarts);
          }
        } else { // I am not the master scanner
          piggy++;
          uint32_t seq = scan_info.seq_num;
          if (seq != UINT_MAX) { // master scanner has already set the sequence number for the scan
            bool res = ActualScan(low, high, seq, result);
            ScanExitProcedure();
            if (res) {
              // FALLBACK
              if (i_am_in_fallback) {
                i_am_in_fallback=false;
                FAD_U64(&fallback_scanners);
              }

              if (this_scan_restarts < 10){
                RECORD_TICK(scan_cdf_array[this_scan_restarts]);          
              } else {
                RECORD_TICK(scan_cdf_array[10]);
              } 

              return Status();
            } else {
              // FALLBACK
              this_scan_restarts++;
              if (this_scan_restarts > 10 && !i_am_in_fallback) {
                i_am_in_fallback = true;
                RECORD_TICK(scan_fallbacks);
                FAI_U64(&fallback_scanners);
              }
              RECORD_TICK(scan_restarts);
              // return Status::NotFound(Slice());
            }
          } else { // seq number is not set yet
            // while (immbuf_ == NULL) {
            //   env_->SleepForMicroseconds(10);
            // }
            // // drain
            seq = scan_info.seq_num;
            while (seq == UINT_MAX) {
              env_->SleepForMicroseconds(10);
              seq = scan_info.seq_num;
            }
            bool res = ActualScan(low, high, seq, result);
            ScanExitProcedure();
            if (res) {
              // FALLBACK
              if (i_am_in_fallback) {
                i_am_in_fallback=false;
                FAD_U64(&fallback_scanners);
              } 

              if (this_scan_restarts < 10){
                RECORD_TICK(scan_cdf_array[this_scan_restarts]);          
              } else {
                RECORD_TICK(scan_cdf_array[10]);
              }            
              return Status();
            } else {
              // FALLBACK
              this_scan_restarts++;
              if (this_scan_restarts > 10 && !i_am_in_fallback) {
                RECORD_TICK(scan_fallbacks);
                i_am_in_fallback = true;
                FAI_U64(&fallback_scanners);
              }
              RECORD_TICK(scan_restarts);
              // return Status::NotFound(Slice());
            }
          }
        }
      } 
    }
  }
}

void DBImpl::ScanExitProcedure () {
  ScanInfo local_scan_info;
  while (true) {
    local_scan_info = scan_info;
    if (local_scan_info.num_scans > 1) { // I am not the last one

      // try to decrease the number of scans
      ScanInfo new_scan_info;
      new_scan_info.num_scans = local_scan_info.num_scans - 1;
      new_scan_info.num_avail_scans = local_scan_info.num_avail_scans;
      new_scan_info.seq_num = local_scan_info.seq_num;
      if (CAS_U64_bool(&(scan_info.comparable), local_scan_info.comparable, new_scan_info.comparable)) {
        return;
      } else {
        continue;
      }
    } else { // I am the last one
      ScanInfo new_scan_info;
      new_scan_info.num_scans = 1;
      new_scan_info.num_avail_scans = 0;
      new_scan_info.seq_num = local_scan_info.seq_num;
      if (CAS_U64_bool(&(scan_info.comparable), local_scan_info.comparable, new_scan_info.comparable)) {
        // i am the last one, do some cleanup
        immbuf_ = NULL;
        // destroy immbuf_
        scan_info.num_scans = 0;

        // resets++;
        // scan_info.num_avail_scans = kMaxNumScans;
        // scan_info.seq_num = UINT_MAX;

        if (local_scan_info.num_avail_scans < 10) {
          resets++;
          scan_info.num_avail_scans = kMaxNumScans;
          scan_info.seq_num = UINT_MAX;
        } else {
          scan_info.num_avail_scans = local_scan_info.num_avail_scans;
        }

        return;
      } else {
        continue;
      }
    }
  }
}

bool DBImpl::MasterScan(const Slice& low, const Slice& high, std::map<Slice, Slice, bool(*)(Slice,Slice)>* result) {

// stop HT bg thread 
  // printf("Pausing bg thread\n");
  pause_membuffer_bg_thread_ = true;

// set sl_write_flag
  sl_write_flag = false;
// create new HT
// make HT -> IMM HT
  immbuf_ = membuf_;
  membuf_ = new MemBuffer(options_.ht_threshold);
  membuf_rcu_wait_others();
  // wait for ongoing writes in SL to finish (RCU)
  sl_write_rcu_wait_others();
  // drain IMM into SL (writers will help drain)
  std::atomic_fetch_add(&drain_helpers_count, 1);
  DrainImmutableMembuffer();
  std::atomic_fetch_add(&drain_done_count, 1);

  while(drain_helpers_count != drain_done_count) {  /*wait*/}
  drain_done_count = 0;
  drain_helpers_count = 0;

  // get seq number SN for scan (simple FAI (fetch and increment))
  uint64_t seq = versions_->FetchAndIncrementLastSequence();
  scan_info.seq_num = seq;
// unset sl_write_flag
  sl_write_flag = true;
// unpause background thread 
  pause_membuffer_bg_thread_ = false;

  return ActualScan(low, high, seq, result);
}

bool DBImpl::ActualScan(const Slice& low, const Slice& high, uint64_t seq, std::map<Slice, Slice, bool(*)(Slice,Slice)>* result) {

  ParsedInternalKey k;

  // memtable
  Iterator* mem_iterator = mem_->NewIterator();
  
  for(mem_iterator->Seek(low); mem_iterator->Valid(); mem_iterator->Next()) {
    ParseInternalKey(mem_iterator->key(), &k);
    // Slice result = ExtractUserKey(mem_iterator->key());
    result->emplace(k.user_key, mem_iterator->value());
    if ((k.user_key).compare2(high) > 0) {
      break;
    }

    // printf("Found on disk %lu \n", SliceToInt(result));
    if ((k.sequence) > seq) {
      return false;
    }
  }

  // immutable memtable
  if (imm_ != NULL) {
    Iterator* imm_iterator = imm_->NewIterator();  
    for(imm_iterator->Seek(low); imm_iterator->Valid(); imm_iterator->Next()) {
      ParseInternalKey(imm_iterator->key(), &k);
      // Slice result = ExtractUserKey(mem_iterator->key());
      result->emplace(k.user_key, imm_iterator->value());
      if ((k.user_key).compare2(high) > 0) {
        break;
      }

      // printf("Found on disk %lu \n", SliceToInt(result));
      if ((k.sequence) > seq) {
        return false;
      }
    }
  }

  // disk
  std::vector<Iterator*> list;
  mutex_.Lock();
  Version* current = versions_->current();
  current->Ref();
  current->AddIterators(ReadOptions(), &list);
  Iterator* disk_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  mutex_.Unlock();

  // next two lines copied from db_iter.cc Seek() function
  std::string low_internal_string;
  AppendInternalKey(
      &low_internal_string, ParsedInternalKey(low, seq, kValueTypeForSeek));
  for(disk_iter->Seek(low_internal_string); disk_iter->Valid(); disk_iter->Next()) {
    ParseInternalKey(disk_iter->key(), &k);
    result->emplace(k.user_key, disk_iter->value());
    if ((k.user_key).compare2(high) > 0) {
      break;
    }


    // printf("Found on disk %lu \n", SliceToInt(result));
    if ((k.sequence) > seq) {
      return false;
    }
  }

  current->Unref();
  return true;
}


Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {

#ifdef ASCY_MEMTABLE
  RECORD_TICK(reads_total)
  // mem_rcu_function_start();
  // imm_rcu_function_start();
  Status s;
  LookupKey lkey(key, kMaxSequenceNumber);

  // ticks start, end;

  // Check membuf_
  //start = getticks();
#ifdef USE_HT
  membuf_rcu_function_start();
  if (membuf_->Get(lkey, value, &s)) { // Look in MemBuffer
    RECORD_TICK(reads_membuffer)
    membuf_rcu_function_end();
    //end = getticks();
    //ht_get_ticks += end - start;
    return s;
  }
  membuf_rcu_function_end(); 
  //end = getticks();
  //ht_get_ticks += end - start;
#endif // USE_HT

  //start = getticks();
  // Check mem_
  mem_rcu_function_start();
  if (mem_->Get(lkey, value, &s)) { // Done
    RECORD_TICK(reads_memtable)
    mem_rcu_function_end();
    //end = getticks();
    //sl_get_ticks += end - start;
    return s;
  }
  
  // Check imm_
  mem_rcu_function_end();
  imm_rcu_function_start();
  if (has_imm_.Acquire_Load() != NULL && imm_->Get(lkey, value, &s)) {
    RECORD_TICK(reads_memtable)
    imm_rcu_function_end();
    //end = getticks();
    //sl_get_ticks += end - start;

    return s;
  } 

  // Check disk
  imm_rcu_function_end();
  //end = getticks();
  //sl_get_ticks += end - start;
  //start = getticks();
    
  // mutex_.Lock();
  Version::GetStats stats;
  Version* current = versions_->current();
  current->Ref();
  // mutex_.Unlock();
  
  s = current->Get(options, lkey, value, &stats);
  
  // mutex_.Lock();
  // current->UpdateStats(stats);
  current->Unref();
  // mutex_.Unlock();

  //end = getticks();
  //disk_get_ticks += end - start;
  return s;
  
#else
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
#endif
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
#ifdef ASCY_MEMTABLE
  RECORD_TICK(updates_total)

  while (sl_total_written_bytes() >= 0.99 * options_.skiplist_size) {
    env_->SleepForMicroseconds(10);
  }

  // try insert in Membuffer
#ifdef USE_HT
  membuf_rcu_function_start();  
  if (!membuf_->Add(kTypeValue, key, val)) {
    membuf_rcu_function_end();

    // Scan in progress, help with drain
#ifdef SCANS
    while (!sl_write_flag || fallback_scanners > 0) {
      if (drain_done_count == 0 && immbuf_ != NULL && fallback_scanners <= 0) {
        std::atomic_fetch_add(&drain_helpers_count, 1);
        
        DrainImmutableMembuffer();
        std::atomic_fetch_add(&drain_done_count, 1);
      } else {
        env_->SleepForMicroseconds(10);
      }
    }
#endif // SCANS
#endif // USE_HT

    uint64_t seq = versions_->FetchAndIncrementLastSequence();
    mem_rcu_function_start();
    sl_write_rcu_function_start();
    mem_->Add(seq, kTypeValue, key, val);
    
    sl_write_rcu_function_end();
    mem_rcu_function_end();
#ifdef USE_HT
  } else {
    membuf_rcu_function_end();
    RECORD_TICK(updates_directly_membuffer)
  }
#endif // USE_HT


  return Status::OK();
#else 
  return DB::Put(o, key, val);
#endif
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
#ifdef ASCY_MEMTABLE

  RECORD_TICK(updates_total)
  Slice dummy_slice;

  while (sl_total_written_bytes() >= 0.99 * options_.skiplist_size) {
    env_->SleepForMicroseconds(10);
  }

#ifdef USE_HT
  // try insert in Membuffer  
  membuf_rcu_function_start();
  if (!membuf_->Add(kTypeDeletion, key, dummy_slice)) {
    membuf_rcu_function_end();

    // Scan in progress, help with drain
#ifdef SCANS
    while (!sl_write_flag) {
      if (drain_done_count == 0 && immbuf_ != NULL) {
        std::atomic_fetch_add(&drain_helpers_count, 1);
        DrainImmutableMembuffer();
        std::atomic_fetch_add(&drain_done_count, 1);
      } else {
        env_->SleepForMicroseconds(10);
      }
    }
#endif
#endif // USE_HT

    uint64_t seq = versions_->FetchAndIncrementLastSequence();
    mem_rcu_function_start();
    sl_write_rcu_function_start();
    mem_->Add(seq, kTypeDeletion, key, dummy_slice);
    sl_write_rcu_function_end();
    mem_rcu_function_end();

#ifdef USE_HT
  } else {
    membuf_rcu_function_end();
    RECORD_TICK(updates_directly_membuffer)
  }
#endif // USE_HT

  return Status::OK();
#else
  return DB::Delete(options, key);
#endif
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
      // impl->MaybeScheduleCompaction();
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
