#if ASCY_MEMTABLE == 1 || ASCY_MEMTABLE == 3
/*
 * File:
 *   intset.h
 * Author(s):
 *   Vincent Gramoli <vincent.gramoli@epfl.ch>
 * Description:
 *   Skip list integer set operations 
 *
 * Copyright (c) 2009-2010.
 *
 * intset.h is part of Synchrobench
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

#ifdef __cplusplus
extern "C" {
#endif

uint64_t sl_contains(sl_intset_t *set, uint64_t key);
uint64_t sl_add(sl_intset_t *set, uint64_t key, uint64_t val, uint64_t seq);
uint64_t sl_remove(sl_intset_t *set, uint64_t key);
int sl_multi_insert(sl_intset_t *set, uint64_t* keys, uint64_t* values, 
    uint64_t* seqs, size_t nkeys, uint64_t* replaced_values);

#ifdef __cplusplus
}
#endif

#endif