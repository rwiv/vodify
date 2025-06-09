#!/bin/bash

cd ..

docker rmi vtask-enc:latest
docker build -t vtask-enc:latest -f ./docker/Dockerfile-enc .
