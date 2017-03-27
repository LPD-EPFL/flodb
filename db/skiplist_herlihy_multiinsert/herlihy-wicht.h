#if ASCY_MEMTABLE == 2

/*
 * File:
 *   fraser.h
 * Author(s):
 *   Vincent Gramoli <vincent.gramoli@epfl.ch>
 * Description:
 *   Lock-based skip list implementation of the Fraser algorithm
 *   "Practical Lock Freedom", K. Fraser, 
 *   PhD dissertation, September 2003
 *   Cambridge University Technical Report UCAM-CL-TR-579 
 *
 * Copyright (c) 2009-2010.
 *
 * fraser.h is part of Synchrobench
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
#include "ssalloc.h"

sval_t herlihy_contains(sl_intset_t* set, skey_t key);
sval_t herlihy_remove(sl_intset_t* set, skey_t key);
sval_t herlihy_add(sl_intset_t* set, skey_t key, sval_t val, uint64_t seq);

int herlihy_multi_insert(sl_intset_t* set, skey_t* keys, uint64_t* values, uint64_t* seqs, size_t nkeys, uint64_t* replaced_values);

#endif // ASCY_MEMTABLE == 2
