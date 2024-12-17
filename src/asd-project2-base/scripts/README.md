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
