#!/bin/bash

contact_address=$1
replica_count=$2

if [ -z "$contact_address" ] || [ -z "$replica_count" ]; then
  echo "Usage: $0 <contact_address> <replica_count>"
  echo "Example: $0 localhost:34000 2"
  exit 1
fi

# Extract port from the contact address
contact_p2p_port=$(echo "$contact_address" | cut -d':' -f2)

# Set the base for the P2P and server ports
base_p2p_port=34000
base_server_port=35000

# Calculate the starting port for the next replica
start_replica_p2p_port=$(($base_p2p_port + $replica_count))

# Calculate server port for the contact
contact_server_port=$(($base_server_port + $contact_p2p_port - $base_p2p_port))

# Launch the contact process
read -p "------------- Press enter to start the contact process. --------------------"
echo "Launching contact process on P2P port $contact_p2p_port, server port $contact_server_port"
java -DlogFilename=logs/contact_node -cp target/asdProj2.jar Main -conf config.properties \
    address=localhost \
    p2p_port=$contact_p2p_port \
    server_port=$contact_server_port \
    initial_membership=$contact_address \
    2>&1 | sed "s/^/[$contact_p2p_port] /" &
sleep 1

# Launch additional replica processes
read -p "------------- Press enter to start replica processes. --------------------"
i=0
while [ $i -lt $replica_count ]; do
  replica_p2p_port=$(($start_replica_p2p_port + $i))
  replica_server_port=$(($base_server_port + $replica_p2p_port - $base_p2p_port))
  echo "Launching replica process on P2P port $replica_p2p_port, server port $replica_server_port"
  java -DlogFilename=logs/replica_node_$replica_p2p_port -cp target/asdProj2.jar Main -conf configLate.properties \
      address=localhost \
      p2p_port=$replica_p2p_port \
      server_port=$replica_server_port \
      contact=$contact_address \
      2>&1 | sed "s/^/[$replica_p2p_port] /" &
  sleep 1
  i=$(($i + 1))
done

# Wait for user to signal shutdown
read -p "------------- Press enter to kill all servers. --------------------"

kill $(ps aux | grep 'asdProj2.jar' | awk '{print $2}')

echo "All processes done!"