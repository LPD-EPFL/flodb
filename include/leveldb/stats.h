#include "db/ascylib_aux/atomic_ops_if.h"
#include <iostream>

#ifndef __STATS_H_
#define __STATS_H_

#ifdef COLLECT_STATS

extern thread_local uint64_t updates_total;
extern thread_local uint64_t updates_directly_membuffer;
extern thread_local uint64_t updates_w_helping_total;
extern thread_local uint64_t updates_w_helping_succ;

extern thread_local uint64_t reads_total;
extern thread_local uint64_t reads_membuffer;
extern thread_local uint64_t reads_memtable;

extern thread_local uint64_t entries_moved;
extern thread_local uint64_t multi_inserts;

extern thread_local uint64_t scan_restarts;
extern thread_local uint64_t scan_fallbacks;
extern thread_local uint64_t scan_cdf_array[11];

extern uint64_t updates_total_all;
extern uint64_t updates_directly_membuffer_all;
extern uint64_t updates_w_helping_total_all;
extern uint64_t updates_w_helping_succ_all;
extern uint64_t reads_total_all;
extern uint64_t reads_membuffer_all;
extern uint64_t reads_memtable_all;

extern uint64_t entries_moved_all;
extern uint64_t multi_inserts_all;

extern uint64_t scan_restarts_all;
extern uint64_t scan_fallbacks_all;
extern uint64_t scan_cdf_array_all[11];


// extern thread_local uint64_t ht_get_ticks;
// extern thread_local uint64_t sl_get_ticks;
// extern thread_local uint64_t disk_get_ticks;
// extern uint64_t ht_get_ticks_all;
// extern uint64_t sl_get_ticks_all;
// extern uint64_t disk_get_ticks_all;


#define RECORD_TICK(target) (target += 1);
#define THREAD_INIT_STATS_VARS ThreadInitStatsVars();
#define GLOBAL_INIT_STATS_VARS GlobalInitStatsVars();
#define COLLECT_THREAD_STATS CollectThreadStats();
#define PRINT_STATS PrintStats();

inline void ThreadInitStatsVars() {
    updates_total = 0;
    updates_directly_membuffer = 0;
    updates_w_helping_total = 0;
    updates_w_helping_succ = 0;
    reads_total = 0;
    reads_membuffer = 0;
    reads_memtable = 0;
    entries_moved = 0;
    multi_inserts = 0;
    scan_restarts = 0;
    scan_fallbacks = 0;

    for (int i = 0; i < 11; i++ ){
        scan_cdf_array[i] = 0;
    }
    // ht_get_ticks = 0;
    // sl_get_ticks = 0;
    // disk_get_ticks = 0;    
}

inline void GlobalInitStatsVars() {
    updates_total_all = 0;
    updates_directly_membuffer_all = 0;
    updates_w_helping_total_all = 0;
    updates_w_helping_succ_all = 0;
    reads_total_all = 0;
    reads_membuffer_all = 0;
    reads_memtable_all = 0;
    entries_moved_all = 0;
    multi_inserts_all = 0;
    scan_restarts_all = 0;
    scan_fallbacks_all = 0;
    for (int i = 0; i < 11; i++ ){
        scan_cdf_array_all[i] = 0;
    }
    // ht_get_ticks_all = 0;
    // sl_get_ticks_all = 0;
    // disk_get_ticks_all = 0;
}

inline void CollectThreadStats() {
    __sync_fetch_and_add(&updates_total_all , updates_total);
    __sync_fetch_and_add(&updates_directly_membuffer_all , updates_directly_membuffer);
    __sync_fetch_and_add(&updates_w_helping_total_all   , updates_w_helping_total);
    __sync_fetch_and_add(&updates_w_helping_succ_all, updates_w_helping_succ);
    __sync_fetch_and_add(&reads_total_all   , reads_total);
    __sync_fetch_and_add(&reads_membuffer_all   , reads_membuffer);
    __sync_fetch_and_add(&reads_memtable_all, reads_memtable);
    __sync_fetch_and_add(&entries_moved_all, entries_moved);
    __sync_fetch_and_add(&multi_inserts_all, multi_inserts);    
    __sync_fetch_and_add(&scan_restarts_all, scan_restarts);    
    __sync_fetch_and_add(&scan_fallbacks_all, scan_fallbacks);

    for (int i = 0; i < 11; i++ ){
        __sync_fetch_and_add(&(scan_cdf_array_all[i]), scan_cdf_array[i]);
    }


    // __sync_fetch_and_add(&ht_get_ticks_all, ht_get_ticks);    
    // __sync_fetch_and_add(&sl_get_ticks_all, sl_get_ticks);    
    // __sync_fetch_and_add(&disk_get_ticks_all, disk_get_ticks);    
}

inline void PrintStats() {
    std::cout << "Updates total: " << updates_total_all << std::endl;
    std::cout << "Updates directly membuffer: " << updates_directly_membuffer_all << std::endl;    
    std::cout << "Updates w/ helping: " << updates_w_helping_total_all << std::endl;
    std::cout << "Uptdates w/ helpin succesful: " << updates_w_helping_succ_all << std::endl;
    std::cout << "Reads total: " << reads_total_all << std::endl;
    std::cout << "Reads from membuffer: " << reads_membuffer_all << std::endl;
    std::cout << "Reads from memtable: " << reads_memtable_all << std::endl;
    std::cout << "Entries Moved: " << entries_moved_all << std::endl;
    std::cout << "Multi-insert calls: " << multi_inserts_all << std::endl;
    std::cout << "Scan restarts: " << scan_restarts_all << std::endl;
    std::cout << "Scan fallbacks: " << scan_fallbacks_all << std::endl;

    std::cout << "Scan CDF: ";
    for (int i = 0; i < 11; i++ ){
        std::cout << scan_cdf_array_all[i] << " ";
    }
    std::cout << std::endl;


    // std::cout << "ht_get_ticks_all: " << ht_get_ticks_all << std::endl;
    // std::cout << "sl_get_ticks_all: " << sl_get_ticks_all << std::endl;
    // std::cout << "disk_get_ticks_all: " << disk_get_ticks_all << std::endl;

}




#else // NO STATS

#define RECORD_TICK(target)
#define THREAD_INIT_STATS_VARS 
#define GLOBAL_INIT_STATS_VARS 
#define COLLECT_THREAD_STATS 
#define PRINT_STATS 

#endif 

#endif // __STATS_H_