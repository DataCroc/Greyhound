#!/bin/bash
​
# start kafka and zookeeper docker
docker-compose up -d
​
# start virtual environment
pipenv shell