# Dev-support using Docker

1. Build s2graph docker image in root directory
	- `sbt docker:publishLocal`
2. docker-compose to run all docker image
	- ```
	cd dev-support
	docker-compose up -d
	```
