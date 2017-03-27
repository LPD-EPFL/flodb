#if ASCY_MEMTABLE == 2

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


sval_t sl_herlihy_contains(sl_intset_t *set, skey_t key);
sval_t sl_herlihy_add(sl_intset_t *set, skey_t key, sval_t val, uint64_t seq);
sval_t sl_herlihy_remove(sl_intset_t *set, skey_t key);

//return value is the number of keys successfully inserted
int sl_herlihy_multi_insert(sl_intset_t *set, skey_t* keys, sval_t* vals, uint64_t* seqs, size_t numkeys, uint64_t* replaced_values);

#ifdef __cplusplus
}
#endif

#endif // ASCY_MEMTABLE == 2