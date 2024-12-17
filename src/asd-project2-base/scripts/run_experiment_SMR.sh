#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <nNodes> {<host1>,...,<hostn>} <resultsDir>"
    exit 1
fi

nNodes=$1
hostsArg="${2#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a hosts <<< "$hostsArg"
resultsDir=$3

# Base ports configuration
base_p2p_port=34000
base_server_port=35000

# Initialize arrays for IPs and membership
declare -a node_ips
membership=""

# First, collect all IPs
echo "Collecting IPs from all nodes..."
for i in $(seq 0 $(($nNodes - 1))); do
    server=${hosts[$(($i % ${#hosts[@]}))]}
    name="asd-$((i + 1))"  # Containers are named asd-1, asd-2, etc.

    # Get IP from container
    echo "Getting IP from $server, container $name..."
    ip=$(ssh $server "docker exec $name hostname -I | awk '{print \$1}'")
    if [ -z "$ip" ]; then
        echo "Error: Unable to retrieve IP for container $name on $server."
        exit 1
    fi
    node_ips[$i]=$ip

    # Build membership string
    if [ $i -eq 0 ]; then
        membership="${ip}:$base_p2p_port"
    else
        membership="$membership,${ip}:$(($base_p2p_port + $i))"
    fi

    echo "Node $i (${name} on ${server}) has IP: ${ip}"
done

echo "Membership string: $membership"

# Create results directory
for server in "${hosts[@]}"; do
    ssh $server "mkdir -p $resultsDir"
done

# Now start processes with collected IPs
for i in $(seq 0 $(($nNodes - 1))); do
    server=${hosts[$(($i % ${#hosts[@]}))]}
    name="asd-$((i + 1))"  # Containers are named asd-1, asd-2, etc.

    cmd="cd /home/asd/abd && \
        java -DlogFilename=$resultsDir/node$(($base_p2p_port + $i)) \
        -cp target/asdProj2.jar Main \
        -conf config.properties \
        address=${node_ips[$i]} \
        p2p_port=$(($base_p2p_port + $i)) \
        server_port=$(($base_server_port + $i)) \
        initial_membership=$membership \
        2>&1 | sed \"s/^/[$(($base_p2p_port + $i))] /\" > $resultsDir/console_$(($base_p2p_port + $i)).log"

    echo "Starting process on $server in container $name with IP ${node_ips[$i]}..."
    ssh $server "docker exec -dt $name sh -c '$cmd'"

    sleep 5s  # Wait briefly between launches
done

echo "All processes started. Logs are being written to $resultsDir."