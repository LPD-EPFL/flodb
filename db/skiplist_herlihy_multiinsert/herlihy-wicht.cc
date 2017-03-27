#if ASCY_MEMTABLE == 2

/*
 * File:
 *   herlihy.c 
 * Author(s):
 *
 * Description:
 * Herlihy, M., Lev, Y., & Shavit, N. (2011). 
 * Concurrent lock-free skiplist with wait-free contains operator. 
 * US Patent 7,937,378, 2(12). 
 * Retrieved from http://www.google.com/patents/US7937378
 *
 * Copyright (c) 2009-2010.
 *
 * fraser.c is part of Synchrobench
 * 
 * Synchrobench is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include "herlihy-wicht.h"
RETRY_STATS_VARS;

#include "latency.h"
#if LATENCY_PARSING == 1
__thread size_t lat_parsing_get = 0;
__thread size_t lat_parsing_put = 0;
__thread size_t lat_parsing_rem = 0;
#endif	/* LATENCY_PARSING == 1 */

int herlihy_find(sl_intset_t* set, skey_t key, sl_node_t** preds, sl_node_t** succs);


extern ALIGNED(CACHE_LINE_SIZE) unsigned int levelmax;
// extern __thread ALIGNED(CACHE_LINE_SIZE) uint64_t key_comps;
// extern __thread ALIGNED(CACHE_LINE_SIZE) uint64_t boosts;

#define MAX_LEVEL 64 /* covers up to 2^64 elements */

int compare_uint64 (const void * a, const void * b);
int compare_keys_vals_seqs (const void * a, const void * b);


typedef struct KeyValSeq {

    uint64_t key;
    uint64_t val;
    uint64_t seq;
} KeyValSeq_t;

sval_t herlihy_add(sl_intset_t* set, skey_t key, sval_t val, uint64_t seq)
{
    uint32_t topLevel = get_rand_level();
    uint32_t level = 0;
    sval_t old_value = 0;

    sl_node_t* preds[MAX_LEVEL + 1];
    sl_node_t* succs[MAX_LEVEL + 1];
            
    sl_node_t* new_element = sl_new_simple_node(key, val, seq, topLevel, 0);


    while(1) {
        if(herlihy_find(set, key, preds, succs)) {
            
            //TODO if found, should replace old val with the new val?
            old_value = SWAP_U64(&(succs[0]->val), val);
            sl_delete_node(new_element, false);
            return old_value;

        } else {
            for (level = 0; level <= topLevel; ++level){
                new_element->next[level] = succs[level];
            }
            
            if(CAS_PTR_bool(&preds[0]->next[0], succs[0], new_element)) {
                // Record bytes written to the skiplist
                sl_add_bytes(sizeof(sl_node_t) + (new_element->toplevel + 1) * sizeof(sl_node_t*));
                for (level = 1; level <= topLevel; ++level){
                    while(1) {

                        if(CAS_PTR_bool(&preds[level]->next[level], succs[level], new_element)){ 
                            break;
                        } else {
                            herlihy_find(set, key, preds, succs);
                        }
                    }
                }

                return old_value;
            }
        }
    }
}

sval_t herlihy_remove(sl_intset_t* set, skey_t key) {
    uint32_t level = 0;

    sl_node_t* preds[MAX_LEVEL + 1];
    sl_node_t* succs[MAX_LEVEL + 1];

    while(1) {
        if(!herlihy_find(set, key, preds, succs)){

            return 0;
        } else {
            sl_node_t* node_to_remove = succs[0];

            for(level = node_to_remove->toplevel; level > 0; --level){
                sl_node_t* succ = NULL;
                do {
                    succ = node_to_remove->next[level];
                    if(IS_MARKED(succ)){
                        break;
                    }
                } while (!CAS_PTR_bool(&node_to_remove->next[level], succ, GET_MARKED(succ)));
            }

            while(1) {
                sl_node_t* succ = node_to_remove->next[0];
                if(IS_MARKED(succ)) {
                    break;
                } else if(CAS_PTR_bool(&node_to_remove->next[0], succ, GET_MARKED(succ))) {
                    
                    herlihy_find(set, key, preds, succs);
                    return 1;
                }
            }
        }
    }
}

sval_t herlihy_contains(sl_intset_t* set, skey_t key) {

    sl_node_t* pred = set->head;
    sl_node_t* curr = NULL;
    sl_node_t* succ = NULL;

    uint32_t level = 0;

    for(level = levelmax; level <= levelmax; --level) {
        curr = GET_UNMARKED(pred->next[level]);

        while(1) {
            succ = curr->next[level];

            while(IS_MARKED(succ)) {
                curr = GET_UNMARKED(curr->next[level]);
                succ = curr->next[level]; 
            }

            if(curr->key < key) {
                pred = curr;
                curr = succ;
            } else {
                break;
            }
        }
    }

    if (curr->key == key) {
        return curr->val;
    } else {
        return 0;
    }
}

int herlihy_find(sl_intset_t* set, skey_t key, sl_node_t** preds, sl_node_t** succs) {
    sl_node_t* pred = NULL;
    sl_node_t* curr = NULL;
    sl_node_t* succ = NULL;
    uint32_t level = 0;
        
retry:
    
    pred = set->head;

    // level is uint so upon reaching 0 it will wrap around to UINT32_MAX
    // which is why level >= 0 is a bad condition.
    for (level = levelmax; level <= levelmax; --level){ 
        curr = pred->next[level];
        // if (GET_UNMARKED(curr)->key < pred->key) {
        //     fprintf(stderr, "VIOLATION: GET_UNMARKED(curr)->key < pred->key!!!!!\n");
        // }

        while(1) {
            if(IS_MARKED(curr)){
                goto retry;
            }

            succ = curr->next[level];

            while(IS_MARKED(succ)){
                if(!CAS_PTR_bool(&pred->next[level], curr, GET_UNMARKED(succ))){
                    goto retry;
                }

                curr = pred->next[level];
                // if (GET_UNMARKED(curr)->key < pred->key) {
                //     fprintf(stderr, "VIOLATION: GET_UNMARKED(curr)->key < pred->key!!!!!\n");
                // }

                if(IS_MARKED(curr)){
                    goto retry;
                }

                succ = curr->next[level];
            }

            // if (IS_MARKED(curr)) {
            //     fprintf(stderr, "VIOLATION: IS_MARKED(curr)!!!!!\n");
            // }

            //key_comps++;
            if(curr->key < key){
                pred = curr;

                
                curr = succ;

            } else {
                break;
            }
        }

        preds[level] = pred;
        succs[level] = curr;
    }

    int found = (curr->key == key);

    return found;
}

int herlihy_find_from_preds(sl_intset_t* set, skey_t key, sl_node_t** old_preds, sl_node_t** preds, sl_node_t** succs) {
    sl_node_t* pred = NULL;
    sl_node_t* curr = NULL;
    sl_node_t* succ = NULL;
    uint32_t level = 0;
        
    pred = set->head;

    for (level = levelmax; level <= levelmax; --level){ 

        if (preds[level]->key > pred->key) {

            pred = preds[level];
            //boosts++;
        }

        curr = pred->next[level];

        while(1) {
            succ = curr->next[level];

            // key_comps++;
            if(curr->key < key){
                pred = curr;                
                curr = succ;
            } else {
                break;
            }
        }

        preds[level] = pred;
        succs[level] = curr;
    }

    int found = (curr->key == key);
    
    return found;
}

int herlihy_multi_insert(sl_intset_t* set, skey_t* keys, uint64_t* values, 
    uint64_t* seqs, size_t nkeys, uint64_t* replaced_values) {

    int i;
    int num_inserted = 0;
    KeyValSeq_t keys_vals_seqs[nkeys];

    for ( i = 0; i < nkeys; i++) {

        keys_vals_seqs[i].key = keys[i];
        keys_vals_seqs[i].val = values[i];
        keys_vals_seqs[i].seq = seqs[i];
        replaced_values[i] = 0;
    }


    qsort (keys_vals_seqs, nkeys, sizeof(KeyValSeq_t), compare_keys_vals_seqs);

    sl_node_t* running_preds[MAX_LEVEL + 1];
    sl_node_t* preds[MAX_LEVEL + 1];


    for (i = 0; i <= MAX_LEVEL; i++) {
        preds[i] = set->head;
    }
    for (i = 0; i < nkeys; i++) {

        // printf("Inserting %d\n", keys[i]);
        uint32_t topLevel = get_rand_level();
        uint32_t level = 0;

        sl_node_t* succs[MAX_LEVEL + 1];
                
        sl_node_t* new_element = sl_new_simple_node( keys_vals_seqs[i].key, keys_vals_seqs[i].val, keys_vals_seqs[i].seq, topLevel, 0);

        while(1) {
            if(herlihy_find_from_preds(set, keys_vals_seqs[i].key, running_preds, preds, succs)) {

                //add old value to list of nodes to be freed and returned
                replaced_values[i] = SWAP_U64(&(succs[0]->val), keys_vals_seqs[i].val);
                sl_delete_node(new_element, false);
                printf("Found %lu; not inserting\n", keys_vals_seqs[i].key);
                break;
            } else {
                for (level = 0; level <= topLevel; ++level){
                    new_element->next[level] = succs[level];
                }
                
                if(CAS_PTR_bool(&preds[0]->next[0], succs[0], new_element)) {

                    sl_add_bytes(sizeof(sl_node_t) + (new_element->toplevel + 1) * sizeof(sl_node_t*));
                    
                    for (level = 1; level <= topLevel; ++level){
                        while(1) {

                            if(CAS_PTR_bool(&preds[level]->next[level], succs[level], new_element)){ 
                                break;
                            } else {
                                herlihy_find_from_preds(set, keys_vals_seqs[i].key, running_preds, preds, succs);
                            }
                        }
                    }
                    
                    num_inserted++;
                    break;
                }
            }

        }

    }

    return num_inserted;
}


int compare_uint64 (const void * a, const void * b) {
  return ( *(uint64_t*)a - *(uint64_t*)b );
}

int compare_keys_vals_seqs (const void * a, const void * b) {
  return ( ((KeyValSeq_t*)a)->key - ((KeyValSeq_t*)b)->key );
}


#endif // ASCY_MEMTABLE == 2