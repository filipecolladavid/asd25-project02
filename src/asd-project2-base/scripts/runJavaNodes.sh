#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <resultFolderID> <nNodes> {<host1>,...,<hostn>} <jarLocation>"
    exit 1
fi

# Arguments
resultsDir=/home/$(whoami)/results/$1
nNodes=$2
hostsArg="${3#\{}"  # Remove leading '{'
hostsArg="${hostsArg%\}}"  # Remove trailing '}'
IFS=',' read -r -a hosts <<< "$hostsArg"
jarLocation=$4  # Should be 'abd' in your case

# Fixed Ports and Parameters
base_p2p_port=34000
base_server_port=35000
network_name=asd2025

# Ensure results directory exists
if [ ! -d "$resultsDir" ]; then
    mkdir -p "$resultsDir" && echo "Created results directory: $resultsDir"
fi

# Generate initial_membership dynamically
membership=""
for i in $(seq 1 $nNodes); do
    membership+="asd-$i:$(($base_p2p_port + $i - 1))"
    if [ $i -lt $nNodes ]; then
        membership+=","
    fi
done

echo "Initial Membership: $membership"

# Iterate through nodes and execute Java command in Docker containers
max=$(( ${#hosts[@]} - 1 ))
s=0

for i in $(seq 1 $nNodes); do
    name=asd-$i
    p2p_port=$(($base_p2p_port + $i - 1))
    server_port=$(($base_server_port + $i - 1))
    server=${hosts[$s]}

    echo "Launching process on node: $name (Host: $server, P2P Port: $p2p_port, Server Port: $server_port)"

    # Command to execute the Java process in the container
    cmd="cd /home/asd/${jarLocation} && java -DlogFilename=logs/node${p2p_port} \
                -cp /home/asd/${jarLocation}/target/asdProj2.jar Main \
                -conf configCluster.properties \
                address=$name p2p_port=${p2p_port} server_port=${server_port} \
                initial_membership=${membership}"

    # SSH into the server, enter the container, and run the Java command
    echo "ssh $server \"docker exec -dt $name sh -c '$cmd'\""
    ssh $server "docker exec -dt $name sh -c '$cmd'"

    # Round-robin through hosts
    s=$((s + 1))
    if [ $s -gt $max ]; then
        s=0
    fi
done

echo "All Java processes launched successfully!"