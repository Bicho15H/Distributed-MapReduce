#!/bin/bash

# ---------- CONFIG ----------
N=20
computers=(02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
login="belhelou-24"
controller_machine="tp-1a226-25"  # Fixed controller machine
local_server_script="Server.py"
local_controller_script="Controller.py"
remote_folder="~/Desktop/experiment"  # Shared folder
dataset_file="/cal/commoncrawl/CC-MAIN-20230321002050-20230321032050-00486.warc.wet"  # dataset to copy to servers

mkdir -p results
echo "tp-1a226-02" > machines.txt

# ----------- DEPLOY SERVERS -----------
echo "[INFO] Deploying server scripts..."
while read -r machine; do
    echo "[INFO] Setting up server on $machine"
    ssh "$login@$machine" "rm -rf $remote_folder; mkdir -p $remote_folder/dataset"
    scp "$local_server_script" "$login@$machine:$remote_folder/"

    echo "[INFO] Copying initial dataset to $machine..."
    ssh "$login@$machine" "cp $dataset_file $remote_folder/dataset/"
done < machines.txt

# ----------- DEPLOY CONTROLLER  -----------
echo "[INFO] Deploying controller script to $controller_machine ..."
ssh "$login@$controller_machine" "mkdir -p $remote_folder"  # just ensure the folder exists
scp "$local_controller_script" "$login@$controller_machine:$remote_folder/"
scp machines.txt "$login@$controller_machine:$remote_folder/"

# ---------- MAIN LOOP ----------
for ((i=1; i<=N; i++)); do
    echo "------------ Running iteration $i ------------"

    # ----------- WRITE MACHINES.TXT -----------
    echo "[INFO] Writing machines.txt with $i machine(s)"
    > machines.txt
    for ((j=0; j<i; j++)); do
        echo "tp-1a226-${computers[$j]}" >> machines.txt
    done

    # ----------- UPDATE CONTROLLER MACHINES.TXT -----------
    scp machines.txt "$login@$controller_machine:$remote_folder/"

    # ----------- START SERVERS -----------
    echo "[INFO] Starting servers..."
    while read -r machine; do
        ssh "-tt" "$login@$machine" "cd $remote_folder; python3 Server.py" &
    done < machines.txt

    sleep 10  # give servers time to start

    # ----------- RUN controller -----------
    echo "[INFO] Running controller..."
    ssh "$login@$controller_machine" "cd $remote_folder; python3 Controller.py" > results/result$i.txt

    wait

    echo "[INFO] Saved controller result to results/result$i.txt"
done

echo "------------ Finished running all iterations ------------"
