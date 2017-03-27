#ifndef _THREAD_LOCAL_H_
#define _THREAD_LOCAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "db/ascylib_aux/utils.h"
#include "assert.h"

struct thread_local_node_t {
    uint64_t mem_rcu_version;
    uint64_t imm_rcu_version;
    uint64_t membuf_rcu_version;
    uint64_t sl_write_rcu_version;
    int64_t sl_bytes_allocated;
    size_t ht_size;
    thread_local_node_t* next;
    uint8_t padding[CACHE_LINE_SIZE - sizeof(mem_rcu_version) - sizeof(imm_rcu_version) - sizeof(membuf_rcu_version) - sizeof(sl_write_rcu_version) - sizeof(sl_bytes_allocated) - sizeof(ht_size)- sizeof(next)];
} ALIGNED(CACHE_LINE_SIZE);

extern __thread thread_local_node_t* thread_local_node ;
extern thread_local_node_t* thread_local_head ;
extern uint64_t nthreads ;

inline void maybe_init() {
    if(unlikely(thread_local_node == NULL)) {
        thread_local_node = (thread_local_node_t*) memalign(CACHE_LINE_SIZE, sizeof(thread_local_node_t));
        assert(thread_local_node != NULL);
        thread_local_node->mem_rcu_version = 0;
        thread_local_node->imm_rcu_version = 0;
        thread_local_node->membuf_rcu_version = 0;
        thread_local_node->sl_write_rcu_version = 0;
        thread_local_node->sl_bytes_allocated = 0;
        thread_local_node->ht_size = 0;
        thread_local_node->next = (thread_local_node_t*) SWAP_PTR(&thread_local_head, thread_local_node);
        uint64_t res = FAI_U64(&nthreads);
        printf("Initializing thread local data: nthreads = %lu, thread_local_head = %p!\n", res, thread_local_head);
    }
}

inline void mem_rcu_function_start() {
    maybe_init();
    thread_local_node->mem_rcu_version++;
}

inline void imm_rcu_function_start() {
    maybe_init();
    thread_local_node->imm_rcu_version++;
}

inline void membuf_rcu_function_start() {
    maybe_init();
    thread_local_node->membuf_rcu_version++;
}

inline void sl_write_rcu_function_start() {
    maybe_init();
    thread_local_node->sl_write_rcu_version++;
}

inline void mem_rcu_function_end() {
    thread_local_node->mem_rcu_version++;
}

inline void imm_rcu_function_end() {
    thread_local_node->imm_rcu_version++;
}

inline void membuf_rcu_function_end() {
    thread_local_node->membuf_rcu_version++;
}

inline void sl_write_rcu_function_end() {
    thread_local_node->sl_write_rcu_version++;
}

void mem_rcu_wait_others() ;
void imm_rcu_wait_others() ;
void membuf_rcu_wait_others() ;
void sl_write_rcu_wait_others() ;

inline void sl_add_bytes(size_t bytes) {
    maybe_init();
    // printf("Adding %zu bytes\n", bytes);
    thread_local_node->sl_bytes_allocated += bytes;
}

inline void sl_subtract_bytes(size_t bytes) {
    maybe_init();
    thread_local_node->sl_bytes_allocated -= bytes;
}

inline void sl_reset_all_bytes() {
    thread_local_node_t* cur = thread_local_head;
    while (cur != NULL) {
        cur->sl_bytes_allocated = 0;
        cur = cur->next;
    }
}

int64_t sl_total_written_bytes() ;

inline void ht_add() {
    maybe_init();
    thread_local_node->ht_size += 1;
}

inline void ht_subtract() {
    maybe_init();
    thread_local_node->ht_size -= 1;
}

int64_t ht_total_size();

#ifdef __cplusplus
}
#endif

#endif //_THREAD_LOCAL_H_

