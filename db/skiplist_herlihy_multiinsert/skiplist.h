#if ASCY_MEMTABLE == 2
/*   
 *   File: skiplist.h
 *   Author: Vincent Gramoli <vincent.gramoli@sydney.edu.au>, 
 *  	     Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: 
 *   skiplist.h is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

#ifndef _SKIPLIST_H_
#define _SKIPLIST_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <cstdint>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>

#include "db/atomic_ops/atomic_ops.h"
#include "atomic_ops_if.h"

#include "common.h"
#include "utils.h"
#include "ssalloc.h"
#include "ssmem.h"
#include "util/thread_local.h"

#define DEFAULT_ELASTICITY              4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               1

extern unsigned int levelmax, size_pad_32, log_base;
extern __thread ssmem_allocator_t* alloc;

#define TRANSACTIONAL                   DEFAULT_ELASTICITY

typedef intptr_t level_t;

typedef volatile struct sl_node
{
  skey_t key;
  sval_t val;
  uint64_t seq; // sequence number for LevelDB compaction
  uint32_t deleted;
  uint32_t toplevel;
  volatile struct sl_node* next[1];

} sl_node_t;

typedef ALIGNED(CACHE_LINE_SIZE) struct sl_intset
{
  sl_node_t* head;
} sl_intset_t;

inline int get_rand_level() {
  
  int i, level = 1;
  for (i = 0; i < levelmax - 1; i++)
    {
      if ((rand_range(101)) < 50) {

        level++;
      }
      else
    break;
    }
  /* 1 <= level <= levelmax */

  return level;
}

int floor_log_2(uint64_t n);
int floor_log_4(uint64_t n);
int floor_log_8(uint64_t n);

sl_node_t* sl_new_simple_node(skey_t key, sval_t val, uint64_t seq, int toplevel, int transactional);
sl_node_t* sl_new_node(skey_t key, sval_t val, sl_node_t* next, int toplevel, int transactional);
void sl_delete_node(sl_node_t *n, bool free_mem);

sl_intset_t* sl_set_new();
void sl_set_delete(sl_intset_t *set);
uint64_t sl_set_size(sl_intset_t *set);

inline long rand_range(long r); /* declared in test.c */

static inline int
is_marked(uintptr_t i)
{
  return (int)(i & (uintptr_t)0x01);
}

static inline uintptr_t
unset_mark(uintptr_t i)
{
  return (i & ~(uintptr_t)0x01);
}

static inline uintptr_t
set_mark(uintptr_t i)
{
  return (i | (uintptr_t)0x01);
}

#define GET_UNMARKED(p) ((sl_node_t*) unset_mark((uintptr_t) (p)))
#define GET_MARKED(p) ((sl_node_t*) set_mark((uintptr_t) (p)))
#define IS_MARKED(p) is_marked((uintptr_t) (p))

#ifdef __cplusplus
}
#endif

#endif	/* _SKIPLIST_H_ */
#endif // ASCY_MEMTABLE == 2