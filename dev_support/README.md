# Dev-support using Docker

1. Build s2graph docker image in root of project directory
	- `sbt docker:publishLocal`
2. Run MySQL and HBase container first.
	- change directory to dev-support. `cd dev-support`
	- `docker-compose up -d graph_mysql` will run MySQL and HBase at same time.
3. Run graph container
	- `docker-compose up -d`

> Graph should be connected with MySQL in initial state. So, you have to run MySQL and HBase before running graph.

## For OS X

In OS X, docker container is running on VirtualBox. If you want to connect with HBase in the docker container from your local machine. You should register the address of docker-machine into the `/etc/hosts` file.

From `docker-compose.yml`, I've supposed the name of docker-machine as `default`. In `/etc/hosts` file, please register IP of docker-machine with name `default`.

```
ex)
192.168.99.100 default
```
