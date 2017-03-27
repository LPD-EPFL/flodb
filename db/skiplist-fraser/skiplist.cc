#if ASCY_MEMTABLE == 5
/*   
 *   File: skiplist.c
 *   Author: Vincent Gramoli <vincent.gramoli@sydney.edu.au>, 
 *  	     Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: 
 *   skiplist.c is part of ASCYLIB
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

#include "skiplist.h"	
#include "leveldb/slice.h"

unsigned int levelmax;
unsigned int log_base = 2;
unsigned int size_pad_32;
__thread ssmem_allocator_t* alloc;
__thread unsigned long * seeds;

// inline int
// get_rand_level()
// {
//   int i, level = 1;
//   for (i = 0; i < levelmax - 1; i++)
//     {
//       if ((rand_range(101)) < (100.0/log_base))
//   	level++;
//       else
//   	break;
//     }
//   /* 1 <= level <= levelmax */

//   return level;
// }

/* 
 * Create a new node without setting its next fields. 
 */
sl_node_t*
sl_new_simple_node(skey_t key, sval_t val, uint64_t seq, int toplevel, int transactional)
{
  sl_node_t *node;

#if GC == 1
  if (unlikely(transactional))
    {
      /* use levelmax instead of toplevel in order to be able to use the ssalloc allocator*/
      size_t ns = size_pad_32;
      size_t ns_rm = ns & 63;
      if (ns_rm)
        {
          ns += 64 - ns_rm;
        }
      node = (sl_node_t*) ssalloc(ns);
    }
  else 
    {
#if defined(TIGHT_ALLOC)
    size_t ns = sizeof(sl_node_t) + toplevel * sizeof(sl_node_t*);
    // if (ns % 32 != 0) {
    //   ns = 32 * (ns/32 + 1);
    // }
#else
      size_t ns = size_pad_32;
#if defined(DO_PAD)
      size_t ns_rm = size_pad_32;
      if (ns_rm)
	{
	  ns += 64 - ns_rm;
	}
#endif
#endif 
  // printf("ALLOCATING %d bytes\n", ns);
      node = (sl_node_t*) malloc(ns);
    }
#else
  /* use levelmax instead of toplevel in order to be able to use the ssalloc allocator*/
  size_t ns = size_pad_32;
  if (transactional)
    {
      size_t ns_rm = ns & 63;
      if (ns_rm)
	{
	  ns += 64 - ns_rm;
	}
    }
  node = (sl_node_t *)ssalloc(ns);
#endif

  if (node == NULL)
    {
      perror("malloc");
      exit(1);
    }

  node->key = key;
  node->val = val;
  node->seq = seq;
  node->toplevel = toplevel;
  node->deleted = 0;

#if defined(__tile__)
  MEM_BARRIER;
#endif

  return node;
}

/* 
 * Create a new node with its next field. 
 * If next=NULL, then this create a tail node. 
 */
sl_node_t*
sl_new_node(skey_t key, sval_t val, sl_node_t *next, int toplevel, int transactional)
{
  volatile sl_node_t *node;
  int i;

  node = sl_new_simple_node(key, val, 0, toplevel, transactional);

  for (i = 0; i < toplevel; i++)
    {
      node->next[i] = next;
    }
	
  MEM_BARRIER;

  return (sl_node_t*) node;
}

void
sl_delete_node(sl_node_t *n, bool free_mem)
{
  /* free(n); */
#if GC == 1
  if (free_mem) {
    if (n->val != TOMBSTONE_VALUE) {
      ((leveldb::Slice *) n->val)->destroy();
    }
  }
  free((void*) n);
#else
  ssfree(n);
#endif
}

sl_intset_t*
sl_set_new()
{
  sl_intset_t *set;
  sl_node_t *min, *max;

  seeds = seed_rand();
  printf("levelmax = %d\n", levelmax);
	
  if ((set = (sl_intset_t *)malloc(sizeof(sl_intset_t))) == NULL)
    {
      perror("malloc");
      exit(1);
    }

  max = sl_new_node(KEY_MAX, 0, NULL, levelmax, 0);
  min = sl_new_node(KEY_MIN, 0, max, levelmax, 0);
  set->head = min;
  return set;
}

void
sl_set_delete(sl_intset_t *set)
{
  sl_node_t *node, *next;

  node = set->head->next[0];
  while (node->next[0] != NULL)
    {
      next = node->next[0];
      sl_delete_node(node, true);
      node = next;
    }
  free(set);
}

uint64_t
sl_set_size(sl_intset_t *set)
{
  uint64_t size = 0;
  sl_node_t *node;

  node = GET_UNMARKED(set->head->next[0]);
  while (node->next[0] != NULL)
    {
      if (!IS_MARKED(node->next[0]))
	{
	  size++;
	}
      node = GET_UNMARKED(node->next[0]);
    }

  return size;
}

#endif // ASCY_MEMTABLE