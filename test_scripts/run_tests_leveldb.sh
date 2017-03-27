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
STATS_FILE_MULTI_BASE=$EXPERIMENT_RUN_DIR/stats-multi

#parameters
nthds="1 2 4 8 16 32"
updates="0 50 100" # 100"
duration="30000" #TODO add loop if testing for more than one value
list_size="50000000" #TODO add loop if testing for more than one value
k_values="15 16 17"



#compile & run HyDB test without multiinsert
system="hydb - no multiinsert"
make -s clean
make -s db_test ASCY_MEMTABLE=1 INIT_SEQ=1 COLLECT_STATS=1

for update in $updates
do
	# outfile="$EXPERIMENT_RUN_NAME-u$update"
	outfile="u$update-throughput"
	touch $EXPERIMENT_RUN_DIR/$outfile
	echo "#$system: -i$list_size -u$update -d$duration" | tee -a $EXPERIMENT_RUN_DIR/$outfile
	echo "#Nthds	#Mops" | tee -a $EXPERIMENT_RUN_DIR/$outfile
	for thd in $nthds
	do
		logfile="u$update-t$thd"
		touch logfile
		echo "#$system: -i$list_size -u$update -d$duration -t$thd" >> $EXPERIMENT_RUN_DIR/$logfile
		./db_test -n$thd -i$list_size -u$update -d$duration | tail -20 >> $EXPERIMENT_RUN_DIR/$logfile
		MOPS=`cat $EXPERIMENT_RUN_DIR/$logfile | grep "#Mops" | grep -o -E '[0-9]+\.[0-9]+'`
		echo "$thd $MOPS" | tee -a $EXPERIMENT_RUN_DIR/$outfile

    done
    echo >> $EXPERIMENT_RUN_DIR/$outfile
    echo >> $EXPERIMENT_RUN_DIR/$outfile
done

#compile & run HyDB with multiinsert
for k in $k_values
do
    system="hydb - multiinsert k=$k"
    make -s clean
    make -s db_test ASCY_MEMTABLE=3 INIT_SEQ=1 COLLECT_STATS=1 CLHT_K=$k

    for update in $updates
    do
        # outfile="$EXPERIMENT_RUN_NAME-u$update"
        outfile="u$update-k$k-throughput"
        touch $EXPERIMENT_RUN_DIR/$outfile
        echo "#$system: -i$list_size -u$update -d$duration" | tee -a $EXPERIMENT_RUN_DIR/$outfile
        echo "#Nthds    #Mops" | tee -a $EXPERIMENT_RUN_DIR/$outfile
        for thd in $nthds
        do
            logfile="k$k-u$update-t$thd-multiinsert"
            touch logfile
            echo "#$system: -i$list_size -u$update -d$duration -t$thd" >> $EXPERIMENT_RUN_DIR/$logfile
            ./db_test -n$thd -i$list_size -u$update -d$duration | tail -20 >> $EXPERIMENT_RUN_DIR/$logfile
            MOPS=`cat $EXPERIMENT_RUN_DIR/$logfile | grep "#Mops" | grep -o -E '[0-9]+\.[0-9]+'`
            echo "$thd $MOPS" | tee -a $EXPERIMENT_RUN_DIR/$outfile

        done
        echo >> $EXPERIMENT_RUN_DIR/$outfile
        echo >> $EXPERIMENT_RUN_DIR/$outfile
    done
done

    
#put clsm results in output file for comparison
for update in $updates
do
    # outfile="$EXPERIMENT_RUN_NAME-u$update"
    outfile="u$update-throughput"
    touch $EXPERIMENT_RUN_DIR/$outfile

    cat "results/clsm/clsm$update" >> $EXPERIMENT_RUN_DIR/$outfile
    echo >> $EXPERIMENT_RUN_DIR/$outfile
    echo >> $EXPERIMENT_RUN_DIR/$outfile
done

# #compile leveldb
system="leveldb"
make clean
make db_test ASCY_MEMTABLE=0 INIT_SEQ=1

#run leveldb test
for update in $updates
do
    # outfile="$EXPERIMENT_RUN_NAME-u$update"
    outfile="u$update-throughput"
    touch $EXPERIMENT_RUN_DIR/$outfile
    echo "#$system: -i$list_size -u$update -d$duration" | tee -a $EXPERIMENT_RUN_DIR/$outfile
    echo "#Nthds    #Mops" | tee -a $EXPERIMENT_RUN_DIR/$outfile
    for thd in $nthds
    do
        logfile="u$update-t$thd-$system"
        touch logfile
        echo "#$system: -i$list_size -u$update -d$duration -t$thd" >> $EXPERIMENT_RUN_DIR/$logfile
        ./db_test -n$thd -i$list_size -u$update -d$duration | tail -20 >> $EXPERIMENT_RUN_DIR/$logfile
        MOPS=`cat $EXPERIMENT_RUN_DIR/$logfile | grep "#Mops" | grep -o -E '[0-9]+\.[0-9]+'`
        echo "$thd $MOPS" | tee -a $EXPERIMENT_RUN_DIR/$outfile

    done
    echo >> $EXPERIMENT_RUN_DIR/$outfile
    echo >> $EXPERIMENT_RUN_DIR/$outfile
done

#stats
rm -rf $STATS_FILE
touch $STATS_FILE
for u in $updates
do
    printf "#U = %d%% \n" $u >> $STATS_FILE
    printf "#%7s|%10s|%10s|%10s|%10s|%10s|%10s\n" "threads" "updates" "drct_upd%" "reads" "membuf%" "memtable%" "Moved" >> $STATS_FILE
    for t in $nthds
    do
        LOGFILE="u$u-t$t"
        printf "%8d " $t >> $STATS_FILE
        UPDATES=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Updates total" | grep -o -E '[0-9]+'`
        UPDATE_DIRECTLY_MEMBUFFER=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Updates directly membuffer" | grep -o -E '[0-9]+'`
        HELP=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "helping" | grep -o -E '[0-9]+'`
        HELP_SUCC=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "succesful" | grep -o -E '[0-9]+'`
        READS=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads total" | grep -o -E '[0-9]+'`
        READS_MEMBUFFER=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads from membuffer" | grep -o -E '[0-9]+'`
        READS_MEMTABLE=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads from memtable" | grep -o -E '[0-9]+'`
        MOVED=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Entries" | grep -o -E '[0-9]+'`

        echo $UPDATES $HELP $HELP_SUCC $READS $READS_MEMBUFFER $READS_MEMTABLE

        HELP_PERC=0
        HELP_SUCC_PERC=0
        UPDATE_DIRECTLY_MEMBUFFER_PERC=0
        
        if test $UPDATES -ne 0; then
            HELP_PERC=`echo "100.0 * $HELP/$UPDATES" | bc -l`
            HELP_SUCC_PERC=`echo "100.0 * $HELP_SUCC/$UPDATES" | bc -l`
            UPDATE_DIRECTLY_MEMBUFFER_PERC=`echo "100.0 * $UPDATE_DIRECTLY_MEMBUFFER/$UPDATES" | bc -l`
        fi

        READS_MEMBUFFER_PERC=0
        READS_MEMTABLE_PERC=0

        if test $READS -ne 0; then
            READS_MEMBUFFER_PERC=`echo "100.0 * $READS_MEMBUFFER/$READS" | bc -l`
            READS_MEMTABLE_PERC=`echo "100.0 * $READS_MEMTABLE/$READS" | bc -l`
        fi




        printf " %9d  %9.3f  %9d  %9.3f  %9.3f  %9d\n" "$UPDATES" "$UPDATE_DIRECTLY_MEMBUFFER_PERC" "$READS" "$READS_MEMBUFFER_PERC" "$READS_MEMTABLE_PERC" "$MOVED" >> $STATS_FILE
    done
    echo >> $STATS_FILE
    echo >> $STATS_FILE
done

for k in $k_values
do

    STATS_FILE_MULTI=$STATS_FILE_MULTI_BASE-k$k
    touch $STATS_FILE_MULTI

    for u in $updates
    do
        printf "#U = %d%% \n" $u >> $STATS_FILE_MULTI
        printf "#%7s|%10s|%10s|%10s|%10s|%10s|%10s|%15s\n" "threads" "updates" "drct_upd%" "reads" "membuf%" "memtable%" "Moved" "Moved/minsert" >> $STATS_FILE_MULTI
        for t in $nthds
        do
            LOGFILE="k$k-u$u-t$t-multiinsert"
            printf "%8d " $t >> $STATS_FILE_MULTI
            UPDATES=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Updates total" | grep -o -E '[0-9]+'`
            UPDATE_DIRECTLY_MEMBUFFER=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Updates directly membuffer" | grep -o -E '[0-9]+'`
            HELP=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "helping" | grep -o -E '[0-9]+'`
            HELP_SUCC=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "succesful" | grep -o -E '[0-9]+'`
            READS=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads total" | grep -o -E '[0-9]+'`
            READS_MEMBUFFER=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads from membuffer" | grep -o -E '[0-9]+'`
            READS_MEMTABLE=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Reads from memtable" | grep -o -E '[0-9]+'`
            MOVED=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Entries" | grep -o -E '[0-9]+'`
            MINSERTS=`cat "$EXPERIMENT_RUN_DIR/$LOGFILE" | grep "Multi-insert" | grep -o -E '[0-9]+'`

            echo $UPDATES $HELP $HELP_SUCC $READS $READS_MEMBUFFER $READS_MEMTABLE

            HELP_PERC=0
            HELP_SUCC_PERC=0
            UPDATE_DIRECTLY_MEMBUFFER_PERC=0
            
            if test $UPDATES -ne 0; then
                HELP_PERC=`echo "100.0 * $HELP/$UPDATES" | bc -l`
                HELP_SUCC_PERC=`echo "100.0 * $HELP_SUCC/$UPDATES" | bc -l`
                UPDATE_DIRECTLY_MEMBUFFER_PERC=`echo "100.0 * $UPDATE_DIRECTLY_MEMBUFFER/$UPDATES" | bc -l`
            fi

            READS_MEMBUFFER_PERC=0
            READS_MEMTABLE_PERC=0

            if test $READS -ne 0; then
                READS_MEMBUFFER_PERC=`echo "100.0 * $READS_MEMBUFFER/$READS" | bc -l`
                READS_MEMTABLE_PERC=`echo "100.0 * $READS_MEMTABLE/$READS" | bc -l`
            fi

            INSERTS_PER_MINSERT=0

            if test $MINSERTS -ne 0; then
                INSERTS_PER_MINSERT=`echo "$MOVED/$MINSERTS" | bc -l`
            fi

            printf " %9d  %9.3f  %9d  %9.3f  %9.3f  %9d  %14.3f\n" "$UPDATES" "$UPDATE_DIRECTLY_MEMBUFFER_PERC" "$READS" "$READS_MEMBUFFER_PERC" "$READS_MEMTABLE_PERC" "$MOVED" "$INSERTS_PER_MINSERT" >> $STATS_FILE_MULTI
        done
        echo >> $STATS_FILE_MULTI
        echo >> $STATS_FILE_MULTI
    done
done



#plotting
# rm -rf $EXPERIMENT_RUN_DIR/plots
# # files=`echo $EXPERIMENT_RUN_DIR`
# files=`find $EXPERIMENT_RUN_DIR -maxdepth 1 -name *-throughput`
# echo $files
# mkdir $EXPERIMENT_RUN_DIR/plots
# for file in $files
# do
# 	filename=`basename $file`
# 	echo $filename
# 	gnuplot -e "filename_='$EXPERIMENT_RUN_DIR/$filename'; outputfile_='$EXPERIMENT_RUN_DIR/plots/$filename.pdf'; title_='leveldb1up'" $SCRIPT_DIR/$PLOT
# done


	

