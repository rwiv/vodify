#!/bin/bash

cd ..

docker rmi vtask-gpu:latest
docker build -t vtask-gpu:latest -f ./docker/Dockerfile-gpu .
