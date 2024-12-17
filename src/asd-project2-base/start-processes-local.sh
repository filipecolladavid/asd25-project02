#!/bin/bash

initial_processes=$1
additional_processes=$2

if [ -z $initial_processes ] || [ $initial_processes -lt 1 ]; then
  echo "please indicate a number of initial processes of at least one"
  exit 0
fi

i=0
base_p2p_port=34000
base_server_port=35000

membership="localhost:${base_p2p_port}"

read -p "------------- Press enter to start initial processes. --------------------"

i=1
while [ $i -lt $initial_processes ]; do
    membership="${membership},localhost:$(($base_p2p_port + $i))"
    i=$(($i + 1))
done
echo ${membership}

# Launch initial processes
i=0
while [ $i -lt $initial_processes ]; do
  java -DlogFilename=logs/node$(($base_p2p_port + $i)) -cp target/asdProj2.jar Main -conf config.properties address=localhost p2p_port=$(($base_p2p_port + $i)) server_port=$(($base_server_port + $i)) initial_membership=$membership 2>&1 | sed "s/^/[$(($base_p2p_port + $i))] /" &
  echo "launched process on p2p port $(($base_p2p_port + $i)), server port $(($base_server_port + $i))"
  sleep 1
  i=$(($i + 1))
done

# Only launch additional processes if a second argument was explicitly provided
if [ ! -z "$additional_processes" ] && [ $additional_processes -gt 0 ]; then
  read -p "------------- Press enter to launch additional processes. --------------------"

  j=0
  while [ $j -lt $additional_processes ]; do
    current_idx=$(($initial_processes + $j))
    java -DlogFilename=logs/node$(($base_p2p_port + $current_idx)) -cp target/asdProj2.jar Main -conf configLate.properties address=localhost p2p_port=$(($base_p2p_port + $current_idx)) server_port=$(($base_server_port + $current_idx)) initial_membership=$membership 2>&1 | sed "s/^/[$(($base_p2p_port + $current_idx))] /" &
    echo "launched additional process on p2p port $(($base_p2p_port + $current_idx)), server port $(($base_server_port + $current_idx))"
    sleep 1
    j=$(($j + 1))
  done
fi

read -p "------------- Press enter to kill all servers. --------------------"

kill $(ps aux | grep 'asdProj2.jar' | awk '{print $2}')

echo "All processes done!"