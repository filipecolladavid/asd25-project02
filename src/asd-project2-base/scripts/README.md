# How to test the application
1. Create the iteractive job using oarth command
2. Run the ```prepare_cluster.sh``` script that moves everything that is necessary to frontend
   1. You might need to create some folders, this is the overall structure
   ```txt
        asd05@frontend:~$ tree
        .
        ├── abd
        │   ├── config.properties
        │   ├── log4j2.xml
        │   └── target
        │       └── asdProj2.jar
        ├── client
        │   ├── asd-client.jar
        │   ├── client.log
        │   ├── config.properties
        │   ├── exec.sh
        │   └── log4j2.xml
        ├── results
        │   ├── asd-1
        │   ├── asd-2
        │   └── asd-3
        └── scripts
            ├── babel_config.properties
            ├── Dockerfile
            ├── inet200Latencies_x0.01.txt
            ├── ips200.txt
            ├── ips.txt
            ├── log4j2.xml
            ├── prepare_cluster.sh
            ├── runDockerNodes.sh
            ├── run_experiment_SMR.sh
            ├── setupTc.sh
            ├── startExperiment.sh
            ├── stopDockerNodes.sh
            ├── stopExperiment.sh
            └── updateDockerImage.sh
    ```
3. Create the swarm
   1. Go to one of the nodes
      1. ```docker swarm leave --force```
      2. ```docker swarm init```
   2. Copy the contents of the init command and execute on the rest of the nodes
         1. You might also need to execute leave command on each node before the ```docker swarm join``` command.
   3. On the docker swarm manager (the one you've executed init), check the nodes
   ```
    asd05@kadabra-06:~$ docker node ls
    ID                            HOSTNAME     STATUS    AVAILABILITY   MANAGER STATUS   ENGINE VERSION
    mnsksiclya0qgsskea4kbwcci *   kadabra-06   Ready     Active         Leader           27.4.0
    egs8iflx9opcglbhwi1aoovfi     kadabra-07   Ready     Active                          27.4.0
    90szd1km2liqn1x3kueal0r7i     kadabra-08   Ready     Active                          27.4.0
    ```
4. Run the ```updateDockerImage.sh```
   1. ```bash ./updateDockerImage.sh 'kadabra-05','kadabra-06','kadabra-07'```
5. Run the ```bash ./runDockerNodes.sh```
   1. ```./runDockerNodes.sh ABD 3 'kadabra-05','kadabra-06','kadabra-07' abd```
   2. ```./runDockerNodes.sh <EXPERIMENTNAME> <NumberNodes> {HOSTS} <ProgramLocation>```
6. Run the ```bash ./runJavaNodes.sh abdResults 3 "{kadabra-06,kadabra-07,kadabra-08}" abd```
   1. ```bash ./runJavaNodes <ResultsFolder> <NumberNodes> {HOSTS} <ProgramLocation>```

## Current state
1. Application is stuck on Initializing<br>
2. Need to create a docker image to run the client<br>
3. I think that we can use asd-1, asd-2 and asd-3 (name of the containers) as addresses<br>
4. Pings to other containers work
```bash
asd-1:/home/asd/abd/logs# tail -f * 
==> node34000.log <==
I[15:16:36,495] [main]HashApp: Listening on 127.0.0.1:35000
D[15:16:36,773] [ServerChannel-1-1]SimpleServerChannel: Server socket ready
D[15:16:36,780] [TCPChannel-5-1]TCPChannel: Server socket ready
I[15:16:36,790] [main]ABD: [127.0.0.1:34000] Initializing ABD

==> node34001.log <==
I[15:16:36,980] [main]HashApp: Listening on 127.0.0.1:35001
D[15:16:37,262] [ServerChannel-1-1]SimpleServerChannel: Server socket ready
D[15:16:37,271] [TCPChannel-5-1]TCPChannel: Server socket ready
I[15:16:37,281] [main]ABD: [127.0.0.1:34001] Initializing ABD

==> node34002.log <==
I[15:16:37,279] [main]HashApp: Listening on 127.0.0.1:35002
D[15:16:37,562] [ServerChannel-1-1]SimpleServerChannel: Server socket ready
D[15:16:37,568] [TCPChannel-5-1]TCPChannel: Server socket ready
I[15:16:37,578] [main]ABD: [127.0.0.1:34002] Initializing ABD
```

```bash
asd-1:/home/asd/abd/logs# ping asd-2
PING asd-2 (10.10.199.38): 56 data bytes
64 bytes from 10.10.199.38: seq=0 ttl=64 time=6.720 ms
64 bytes from 10.10.199.38: seq=1 ttl=64 time=6.736 ms
64 bytes from 10.10.199.38: seq=2 ttl=64 time=6.557 ms
64 bytes from 10.10.199.38: seq=3 ttl=64 time=6.557 ms
64 bytes from 10.10.199.38: seq=4 ttl=64 time=6.578 ms
64 bytes from 10.10.199.38: seq=5 ttl=64 time=6.841 ms
64 bytes from 10.10.199.38: seq=6 ttl=64 time=6.639 ms
```