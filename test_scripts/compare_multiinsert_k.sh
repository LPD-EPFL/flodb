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
STATS_FILE=$EXPERIMENT_RUN_DIR/stats
STATS_FILE_MULTI=$EXPERIMENT_RUN_DIR/stats-multi

#parameters
nthds="16" #"1 2 4 8 16 32"
updates="0 50 100"
duration="30000" #TODO add loop if testing for more than one value
list_size="200000000" #TODO add loop if testing for more than one value
k_values="15 16 17"

for k in $k_values
do
    make -s clean; make -s db_test ASCY_MEMTABLE=3 INIT_SEQ=1 COLLECT_STATS=1 CLHT_K=$k
    for update in $updates
    do
        logfile="k$k-u$update"
        touch $EXPERIMENT_RUN_DIR/$logfile
        echo "#CLHT_K=$k -i$list_size -u$update -d$duration" | tee -a $EXPERIMENT_RUN_DIR/$logfile
        ./db_test -n$nthds -i$list_size -u$update -d$duration | tee /dev/tty | tail -20 >> $EXPERIMENT_RUN_DIR/$logfile
    done
done

# run the no-multiinsert case
make -s clean; make -s db_test ASCY_MEMTABLE=1 INIT_SEQ=1 COLLECT_STATS=1
for update in $updates
do
    logfile="no-multinsert-u$u"
    touch $EXPERIMENT_RUN_DIR/$logfile
    echo "no-multiinsert -i$list_size -u$update -d$duration" | tee -a $EXPERIMENT_RUN_DIR/$logfile
    ./db_test -n$thd -i$list_size -u$update -d$duration | tee /dev/tty | tail -20 >> $EXPERIMENT_RUN_DIR/$logfile
done

    

