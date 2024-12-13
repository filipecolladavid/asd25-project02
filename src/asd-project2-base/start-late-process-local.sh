#!/bin/bash

contact_port=$1
current_replicas=$2

if [ -z $contact_port ] || [ -z $current_replicas ]; then
  echo "Usage: $0 <contact_port> <current_replicas>"
  echo "Example: $0 34000 3"
  exit 1
fi

base_p2p_port=34000
base_server_port=35000

# Calculate the new process ports based on current replicas
new_p2p_port=$(($base_p2p_port + $current_replicas))
new_server_port=$(($base_server_port + $current_replicas))

# Store the process ID in a variable for later termination
java -DlogFilename=logs/node${new_p2p_port} \
     -cp target/asdProj2.jar Main \
     -conf configLate.properties \
     address=localhost \
     p2p_port=${new_p2p_port} \
     server_port=${new_server_port} \
     initial_membership=localhost:${contact_port} 2>&1 | sed "s/^/[${new_p2p_port}] /" &

# Store the PID
PROCESS_PID=$!
echo $PROCESS_PID > .current_process_pid

echo "Launched process on p2p port ${new_p2p_port}, server port ${new_server_port}"
echo "Process ID: $PROCESS_PID"
echo "To kill this specific process, you can either:"
echo "1. Run: kill $PROCESS_PID"
echo "2. Run: kill \$(cat .current_process_pid)"

read -p "------------- Press enter to kill the process. --------------------"

if kill $PROCESS_PID; then
  echo "Process successfully terminated"
  rm .current_process_pid
else
  echo "Process already terminated"
  rm -f .current_process_pid
fi