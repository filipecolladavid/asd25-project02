#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "$0 <resultFolderID> <nNodes> {<host1>,...,<hostn>} <jarLocation>"
    exit 1
fi

resultsDir=/home/$(whoami)/results/$1
if [ ! -d $resultsDir ]; then
  mkdir $resultsDir && echo "created $resultsDir"
fi

nNodes=$2
hostsArg="${3#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a hosts <<< "$hostsArg"

swarmManager=${hosts[0]}

cpu=1
net=asd2025
bandwidth=100
image=asd

jarDir=/home/$(whoami)/$4
if [ ! -d $jarDir ]; then
  mkdir -p $jarDir && echo "created $jarDir"
fi

clientDir=/home/$(whoami)/client
if [ ! -d $clientDir ]; then
  mkdir -p $clientDir && echo "created $clientDir"
fi

IFS=$'\n' read -d '' -r -a ips < ./ips200.txt

max=$(( ${#hosts[@]} - 1 ))

ssh $swarmManager "docker network rm $net --force"
ssh $swarmManager "docker network create $net -d overlay --attachable --subnet 10.10.0.0/16 --gateway 10.10.0.1"

s=0

for i in $(seq 1 $nNodes)
do
    if [ $i -eq 1 ]; then
        name=client
        ip=${ips[$i-1]}
        server=${hosts[$s]}
        echo $name $ip $server
        if [ ! -d ${resultsDir}/${name} ]; then
          mkdir ${resultsDir}/${name} && echo "created ${resultsDir}/${name}"
        fi

        echo "ssh $server \"docker run --rm -d -t --cpus=$cpu --privileged -v $clientDir:/home/asd/client -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image 1 $bandwidth\""
        ssh $server "docker run --rm -d -t --cpus=$cpu --privileged -v $clientDir:/home/asd/client -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image 1 $bandwidth"
    else
        name=asd-$((i-1))
        ip=${ips[$i-1]}
        server=${hosts[$s]}
        echo $name $ip $server
        if [ ! -d ${resultsDir}/${name} ]; then
          mkdir ${resultsDir}/${name} && echo "created ${resultsDir}/${name}"
        fi

        echo "ssh $server \"docker run --rm -d -t --cpus=$cpu --privileged -v $jarDir:/home/asd/$4 -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image $i $bandwidth\""
        ssh $server "docker run --rm -d -t --cpus=$cpu --privileged -v $jarDir:/home/asd/$4 -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image $i $bandwidth"
    fi

    s=$(( $s + 1 ))
    if [ $s -gt $max ]; then
        s=0
    fi
done