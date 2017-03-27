#include "util/thread_local.h"

__thread thread_local_node_t* thread_local_node = NULL;
thread_local_node_t* thread_local_head = NULL;
uint64_t nthreads = 0;

void mem_rcu_wait_others() {
    // printf("mem_rcu_wait_others nthreads = %lu \n", nthreads);
    thread_local_node_t* cur = thread_local_head;
    uint64_t ref_versions[nthreads];

    // Gather reference versions first
    int i = 0;
    while (cur != NULL) {
        ref_versions[i] = cur->mem_rcu_version;
        i++;
        cur = cur->next;
    }

    bool everyone_seen;
    do {
        everyone_seen = true;
        i = 0;
        cur = thread_local_head;
        while (cur != NULL) {
            if ((cur->mem_rcu_version % 2 == 1) 
                && (cur->mem_rcu_version <= ref_versions[i])) {
                everyone_seen = false;
            }
            i++;
            cur = cur->next;
        }
    } while(!everyone_seen);
}

void imm_rcu_wait_others() {
    // printf("imm_rcu_wait_others nthreads = %lu \n", nthreads);
    thread_local_node_t* cur = thread_local_head;
    uint64_t ref_versions[nthreads];

    // Gather reference versions first
    int i = 0;
    while (cur != NULL) {
        ref_versions[i] = cur->imm_rcu_version;
        i++;
        cur = cur->next;
    }

    bool everyone_seen;
    do {
        everyone_seen = true;
        i = 0;
        cur = thread_local_head;
        while (cur != NULL) {
            if ((cur->imm_rcu_version % 2 == 1) 
                && (cur->imm_rcu_version <= ref_versions[i])) {
                everyone_seen = false;
            }
            i++;
            cur = cur->next;
        }
    } while(!everyone_seen);
}

void membuf_rcu_wait_others() {
    // printf("membuf_rcu_wait_others nthreads = %lu \n", nthreads);
    thread_local_node_t* cur = thread_local_head;
    uint64_t ref_versions[nthreads];

    // Gather reference versions first
    int i = 0;
    while (cur != NULL) {
        ref_versions[i] = cur->membuf_rcu_version;
        i++;
        cur = cur->next;
    }

    bool everyone_seen;
    do {
        everyone_seen = true;
        i = 0;
        cur = thread_local_head;
        while (cur != NULL) {
            if ((cur->membuf_rcu_version % 2 == 1) 
                && (cur->membuf_rcu_version <= ref_versions[i])) {
                everyone_seen = false;
            }
            i++;
            cur = cur->next;
        }
    } while(!everyone_seen);
}

void sl_write_rcu_wait_others() {
    // printf("sl_write_rcu_version nthreads = %lu \n", nthreads);
    thread_local_node_t* cur = thread_local_head;
    uint64_t ref_versions[nthreads];

    // Gather reference versions first
    int i = 0;
    while (cur != NULL) {
        ref_versions[i] = cur->sl_write_rcu_version;
        i++;
        cur = cur->next;
    }

    bool everyone_seen;
    do {
        everyone_seen = true;
        i = 0;
        cur = thread_local_head;
        while (cur != NULL) {
            if ((cur->sl_write_rcu_version % 2 == 1) 
                && (cur->sl_write_rcu_version <= ref_versions[i])) {
                everyone_seen = false;
            }
            i++;
            cur = cur->next;
        }
    } while(!everyone_seen);
}


int64_t sl_total_written_bytes() {
    thread_local_node_t* cur = thread_local_head;
    int64_t res = 0;
    int i = 0;
    while (cur != NULL) {
        res += cur->sl_bytes_allocated;
        cur = cur->next;
        i++;
    }
    // printf("sl_total_written_bytes: %lu threads, %d nodes\n", nthreads, i);
    return res;
}

int64_t ht_total_size() {
    thread_local_node_t* cur = thread_local_head;
    int64_t res = 0;
    int i = 0;
    while (cur != NULL) {
        res += cur->ht_size;
        cur = cur->next;
        i++;
    }
    // printf("sl_total_written_bytes: %lu threads, %d nodes\n", nthreads, i);
    return res;
}