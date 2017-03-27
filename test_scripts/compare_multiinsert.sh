#!/bin/sh

NKEYS="500 50 5" #50 500 5000"
DURS="5000" #60000 120000"
INITIAL="50000000"
RANGE="200000000"
THREADS="1 2 4"
NEIGHBORHOOD_MULTIPLIER="2 20 200 2000"

echo "keys initial neighborhood threads: MOPS_SIMPLE MOPS_MULTI PERC_CHANGE% SIZE_AFTER_SIMPLE SIZE_AFTER_MULTI PERC_CHANGE_KEY_COMPS BOOSTS_PER_KEY"
for n in $NKEYS
do
    for m in $NEIGHBORHOOD_MULTIPLIER
    do
        neighborhood=`expr $n \* $m`
        for t in $THREADS
        do

            for i in $INITIAL
            do
                OUTPUT_SIMPLE=`./bin/lf-sl_herlihy_wicht -u100 -n$t -i$i -r$RANGE -d$DURS`
                MOPS_SIMPLE=`echo "$OUTPUT_SIMPLE" | grep "#Mops" | grep -o -E '[0-9]+\.[0-9]+'`
                SIZE_AFTER_SIMPLE=`echo "$OUTPUT_SIMPLE" | grep "#AFTER" | grep -o -E '[0-9]+'`
                KEY_COMPS_SIMPLE=`echo "$OUTPUT_SIMPLE" | grep "#Key" | grep -o -E '[0-9]+'`


                OUTPUT_MULTI=`./bin/lf-sl_herlihy_wicht -u100 -n$t -i$i -d$DURS -r$RANGE -m$NKEYS -s$neighborhood`
                MOPS_MULTI=`echo "$OUTPUT_MULTI" | grep "#Mops" | grep -o -E '[0-9]+\.[0-9]+'`
                SIZE_AFTER_MULTI=`echo "$OUTPUT_MULTI" | grep "#AFTER" | grep -o -E '[0-9]+'`
                KEY_COMPS_MULTI=`echo "$OUTPUT_MULTI" | grep "#Key" | grep -o -E '[0-9]+'`
                BOOSTS=`echo "$OUTPUT_MULTI" | grep "#Boosts" | grep -o -E '[0-9]+'`

                PERC_CHANGE=`echo "scale=2; 100.0 * ($MOPS_MULTI-$MOPS_SIMPLE)/$MOPS_SIMPLE" | bc`
                PERC_CHANGE_KEY_COMPS=`echo "scale=2; 100.0 * ($KEY_COMPS_MULTI-$KEY_COMPS_SIMPLE)/$KEY_COMPS_SIMPLE" | bc`
                BOOSTS_PER_KEY=`echo "scale=2; $BOOSTS/($MOPS_MULTI*$DURS*1000)" | bc`

                echo "$n $i $neighborhood $t: $MOPS_SIMPLE $MOPS_MULTI $PERC_CHANGE% $SIZE_AFTER_SIMPLE $SIZE_AFTER_MULTI $PERC_CHANGE_KEY_COMPS% $BOOSTS_PER_KEY" 
            done
        done
    done
done