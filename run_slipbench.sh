#!/usr/bin/env bash

export LD_LIBRARY_PATH="/usr/local/lib/:$LD_LIBRARY_PATH"

# Utility functions
function fail {
	echo "Failure encountered! Exiting!"
	exit -1
}

function e {
	echo -e '\E[37;44m'"\033[1m$1\033[0m" 
}

if [ $# -ne 1 ]; then
echo "Usage: run_slipbench.sh <config file>"
echo "Please specify a configuration file!"
fail
fi

source ./$1

if [ -z $ROOT_DIR ]; then
echo "Missing configuration!"
fail
fi

clush -w ${GROUP[1]} -l $USER "$SUDO rm -f $ROOT_DIR/fill_test_data"

# Benchmark run
for m in $MEMORIES; do
for input in $INPUTS; do
for i in $NUM_MACHINES; do
names=""
ports=""
ifaces=""

for (( j=0; j<$i; j++ )); do
names="${names}name$j=${MACHINE[$j]}
"
ports="${ports}base_port$j=5555
"
ifaces="${ifaces}iface$j=${MACHINE[$j]}
"
done
for (( j=0; j<$i; j++ )); do
ssh $USER@${MACHINE[$j]} "echo $j > $ROOT_DIR/id"
ssh $USER@${MACHINE[$j]} "cat << EOF > $ROOT_DIR/hurricane.ini
[machines]
count=$i
me=$j
${names}${ifaces}
EOF"
ssh $USER@${MACHINE[$j]} "cat << EOF > $ROOT_DIR/global.sh
ROOT_DIR=${ROOT_DIR}
RESULTS_FOLDER=${RESULTS_FOLDER}
BLOCKED_MEMORY=${BLOCKED_MEMORY}
EOF"
done

sleep 10

e "Running on ${GROUP[$i]}"

for j in $CORES; do
for additional in $ADDITIONALS; do
for alg in $ALGS; do

e "Killing all processes..."
clush -w ${GROUP[$i]} -l $USER "$SUDO $ROOT_DIR/helpers/killall.sh $i"
sleep 10

clush -w ${GROUP[$i]} -l $USER "$SUDO sync && $SUDO echo 3 > /proc/sys/vm/drop_caches"
clush -w ${GROUP[$i]} -l $USER "$SUDO test -f $ROOT_DIR/fill_test_data || $SUDO mv $ROOT_DIR/fill_data $ROOT_DIR/fill_test_data"

e "Running $alg with n=$i, ncpus=$j, mem=$m"
clush -w ${GROUP[$i]} -l $USER "$SUDO $ROOT_DIR/helpers/bench-run.sh $ROOT_DIR $alg $i $j $m $additional"

clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/chkp* && rm -f $ROOT_DIR/snap*"
done
done
done

if [[ "$input" == rmat* ]]; then
clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/rmat-*"
fi;

clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/id && $SUDO rm -f $ROOT_DIR/hurricane.ini && $SUDO rm -f $ROOT_DIR/global.sh"
clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/fill_data"
clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/fill_test_data"
clush -w ${GROUP[$i]} -l $USER "$SUDO rm -f $ROOT_DIR/chkp* && $SUDO rm -f $ROOT_DIR/snap*"
done
done
done

e "Benchmarks done!"

