#include "leveldb/stats.h"

#ifdef COLLECT_STATS

__thread uint64_t updates_total;
__thread uint64_t updates_directly_membuffer;
__thread uint64_t updates_w_helping_total;
__thread uint64_t updates_w_helping_succ;
__thread uint64_t reads_total;
__thread uint64_t reads_membuffer;
__thread uint64_t reads_memtable;
__thread uint64_t entries_moved;
__thread uint64_t multi_inserts;
__thread uint64_t scan_restarts;
__thread uint64_t scan_fallbacks;
__thread uint64_t scan_cdf_array[11];

uint64_t updates_total_all;
uint64_t updates_directly_membuffer_all;
uint64_t updates_w_helping_total_all;
uint64_t updates_w_helping_succ_all;
uint64_t reads_total_all;
uint64_t reads_membuffer_all;
uint64_t reads_memtable_all;
uint64_t entries_moved_all;
uint64_t multi_inserts_all;
uint64_t scan_restarts_all;
uint64_t scan_fallbacks_all;
uint64_t scan_cdf_array_all[11];

// __thread uint64_t ht_get_ticks;
// __thread uint64_t sl_get_ticks;
// __thread uint64_t disk_get_ticks;
// uint64_t ht_get_ticks_all;
// uint64_t sl_get_ticks_all;
// uint64_t disk_get_ticks_all;

#endif