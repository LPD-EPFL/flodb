#if ASCY_MEMTABLE == 1 || ASCY_MEMTABLE == 3
/*
 * File:
 *   skiplist.c
 * Author(s):
 *   Vincent Gramoli <vincent.gramoli@epfl.ch>
 * Description:
 *   Skip list definition 
 *
 * Copyright (c) 2009-2010.
 *
 * skiplist.c is part of Synchrobench
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

#include "skiplist.h"	
#include "leveldb/slice.h"

unsigned int levelmax;
unsigned int size_pad_32;
__thread ssmem_allocator_t* alloc;
__thread unsigned long * seeds;

// int
// floor_log_2(unsigned int n)
// {
//   int pos = 0;
//   if (n >= 1<<16) { n >>= 16; pos += 16; }
//   if (n >= 1<< 8) { n >>=  8; pos +=  8; }
//   if (n >= 1<< 4) { n >>=  4; pos +=  4; }
//   if (n >= 1<< 2) { n >>=  2; pos +=  2; }
//   if (n >= 1<< 1) {           pos +=  1; }
//   return ((n == 0) ? (-1) : pos);
// }

/* 
 * Create a new node without setting its next fields. 
 */
sl_node_t*
sl_new_simple_node(uint64_t key, int toplevel, int transactional)
{
  sl_node_t *node;


  /* use levelmax instead of toplevel in order to be able to use the ssalloc allocator*/
  size_t ns = size_pad_32;
  if (transactional) {
      size_t ns_rm = ns & 63;
      if (ns_rm) {
	        ns += 64 - ns_rm;
	    }
      // node = (sl_node_t*) ssalloc(ns);
      node = (sl_node_t*) malloc(ns);
  } else {
    // node = (sl_node_t *)ssmem_alloc(alloc, ns);
    ns = sizeof(sl_node_t) + ((toplevel+1) * sizeof(sl_node_t *));
    node = (sl_node_t *) malloc(ns);
  }
  // printf("Allocating node of size %zu @ %p\n", ns, node);

  if (node == NULL) {
      perror("malloc");
      exit(1);
  }

  node->key = key;
  node->toplevel = toplevel;
  node->deleted = 0;

  return node;
}

/* 
 * Create a new node with its next field. 
 * If next=NULL, then this create a tail node. 
 */
sl_node_t*
sl_new_node(uint64_t key, sl_node_t *next, int toplevel, int transactional)
{
  volatile sl_node_t *node;
  int i;

  node = sl_new_simple_node(key, toplevel, transactional);

  for (i = 0; i <= levelmax; i++)
    {
      node->next[i] = next;
    }
	
  MEM_BARRIER;

  return (sl_node_t*) node;
}

void
sl_delete_node(sl_node_t *n, bool free_mem)
{
  // free((void*) n);
  if (free_mem) {
    if (n->val != 0 && n->val != TOMBSTONE_VALUE) {
      ((leveldb::Slice *) n->val)->destroy();
    }
  }
  free((void*) n);
}

sl_intset_t*
sl_set_new()
{
  sl_intset_t *set;
  sl_node_t *min, *max;
	
  if ((set = (sl_intset_t *)ssalloc_aligned(CACHE_LINE_SIZE, sizeof(sl_intset_t))) == NULL)
    {
      perror("malloc");
      exit(1);
    }

  // max = sl_new_node(KEY_MAX, NULL, levelmax, 1);
  // min = sl_new_node(KEY_MIN, max, levelmax, 1);

  max = sl_new_node(UINT64_MAX, NULL, levelmax, 1);
  min = sl_new_node(0, max, levelmax, 1);

  max->val = 0;
  max->seq = 0;

  min->val = 0;
  min->seq = 0;

  set->head = min;
  return set;
}

void
sl_set_delete(sl_intset_t *set)
{
  sl_node_t *node, *next;

  node = set->head;
  while (node != NULL)
    {
      next = node->next[0];
      sl_delete_node(node, true);
      node = next;
    }
  ssfree((void*) set);
}

int
sl_set_size(sl_intset_t *set)
{
  int size = 0;
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

#endif