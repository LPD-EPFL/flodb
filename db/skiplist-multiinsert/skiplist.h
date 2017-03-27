#if ASCY_MEMTABLE == 1 || ASCY_MEMTABLE == 3
#ifndef _SKIPLIST_H_
#define _SKIPLIST_H_

/*
 * File:
 *   skiplist.h
 * Author(s):
 *   Vincent Gramoli <vincent.gramoli@epfl.ch>
 * Description:
 *   Stress test of the skip list implementation.
 *
 * Copyright (c) 2009-2010.
 *
 * skiplist.h is part of Synchrobench
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

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>

#include <atomic_ops.h>
#include "atomic_ops_if.h"

#include "common.h"
#include "utils.h"
#include "ssalloc.h"
#include "ssmem.h"

#define DEFAULT_ELASTICITY              4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               1

extern unsigned int levelmax, size_pad_32;
extern __thread ssmem_allocator_t* alloc;

#define TRANSACTIONAL                   DEFAULT_ELASTICITY

typedef intptr_t level_t;

typedef volatile struct sl_node
{
  uint64_t key;
  uint64_t val;
  uint64_t seq;
  uint32_t deleted;
  uint32_t toplevel;
  volatile struct sl_node* next[1];

} sl_node_t;

typedef ALIGNED(CACHE_LINE_SIZE) struct sl_intset
{
  sl_node_t* head;
} sl_intset_t;

inline int
get_rand_level()
{
  int i, level = 1;
  for (i = 0; i < levelmax - 1; i++)
    {
      if ((rand_range(101)) < 50)
    level++;
      else
    break;
    }
  /* 1 <= level <= levelmax */

  return level;
}

sl_node_t* sl_new_simple_node(uint64_t key, int toplevel, int transactional);
sl_node_t* sl_new_node(uint64_t key, sl_node_t* next, int toplevel, int transactional);
void sl_delete_node(sl_node_t *n, bool free_mem);

sl_intset_t* sl_set_new();
void sl_set_delete(sl_intset_t *set);
int sl_set_size(sl_intset_t *set);

inline long rand_range(long r); /* declared in test.c */

static inline int
is_marked(uint64_t i)
{
  return (int)(i & (uint64_t)0x01);
}

static inline uint64_t
unset_mark(uint64_t i)
{
  return (i & ~(uint64_t)0x01);
}

static inline uint64_t
set_mark(uint64_t i)
{
  return (i | (uint64_t)0x01);
}

#define GET_UNMARKED(p) ((sl_node_t*) unset_mark((uint64_t) (p)))
#define GET_MARKED(p) ((sl_node_t*) set_mark((uint64_t) (p)))
#define IS_MARKED(p) is_marked((uint64_t) (p))

#ifdef __cplusplus
}
#endif

#endif	/* _SKIPLIST_H_ */
#endif