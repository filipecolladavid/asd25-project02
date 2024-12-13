processes:
	mvn clean compile package && ./start-processes-local.sh 3
lateJoin:
	mvn clean compile package && ./start-processes-local.sh 3 1
single:
	mvn clean compile package && ./start-late-process-local.sh localhost:35000 3
clients:
	client/./exec.sh 3 1024 localhost:35000,localhost:35001,localhost:35002 1 1
