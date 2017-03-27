#!/bin/sh


#create Test Folders
SCRIPT_DIR="test_scripts"
RESULTS_DIR="results"
TEST_DATE_DIR=`date '+%d-%m'`
TEST_TIME_DIR=`date '+%H:%M:%S'`
EXPERIMENT_RUN_DIR="$RESULTS_DIR/$TEST_DATE_DIR/$TEST_TIME_DIR"
EXPERIMENT_RUN_NAME=`date '+%H:%M:%S'`
PLOT="plot_throughput.gp"
mkdir $RESULTS_DIR/$TEST_DATE_DIR
mkdir $EXPERIMENT_RUN_DIR

EXPERIMENT_RUN_DIR="results/10-11/12:00:23"
STATS_FILE=$EXPERIMENT_RUN_DIR/stats

#parameters
nthds="1 2 4 8 16 32"
updates="0 10 50 100" # 50 100"
duration="30000" #TODO add loop if testing for more than one value
list_size="200000000" #TODO add loop if testing for more than one value

rm -rf $STATS_FILE
touch $STATS_FILE
for u in $updates
do
    printf "#U = %d%% \n" $u >> $STATS_FILE
    printf "#%7s|%10s|%10s|%10s|%10s|%10s|%10s\n" "threads" "updates" "help%" "help_suc%" "reads" "membuf%" "memtable%" >> $STATS_FILE
    for t in $nthds
    do
        printf "%8d|" $t >> $STATS_FILE
        UPDATES=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "Updates total" | grep -o -E '[0-9]+'`
        HELP=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "helping" | grep -o -E '[0-9]+'`
        HELP_SUCC=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "succesful" | grep -o -E '[0-9]+'`
        READS=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "Reads total" | grep -o -E '[0-9]+'`
        READS_MEMBUFFER=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "Reads from membuffer" | grep -o -E '[0-9]+'`
        READS_MEMTABLE=`cat "$EXPERIMENT_RUN_DIR/u$u-t$t" | grep "Reads from memtable" | grep -o -E '[0-9]+'`

        echo $UPDATES $HELP $HELP_SUCC $READS $READS_MEMBUFFER $READS_MEMTABLE

        HELP_PERC=`echo "100.0 * $HELP/$UPDATES" | bc -l`
        HELP_SUCC_PERC=`echo "100.0 * $HELP_SUCC/$UPDATES" | bc -l`
        READS_MEMBUFFER_PERC=0
        READS_MEMTABLE_PERC=0

        if test $READS -ne 0; then
            READS_MEMBUFFER_PERC=`echo "100.0 * $READS_MEMBUFFER/$READS" | bc -l`
            READS_MEMTABLE_PERC=`echo "100.0 * $READS_MEMTABLE/$READS" | bc -l`
        fi

        printf " %9d| %9.3f| %9.3f| %9d| %9.3f| %9.3f\n" "$UPDATES" "$HELP_PERC" "$HELP_SUCC_PERC" "$READS" "$READS_MEMBUFFER_PERC" "$READS_MEMTABLE_PERC" >> $STATS_FILE
    done
    echo >> $STATS_FILE
    echo >> $STATS_FILE
done
