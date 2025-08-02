#!/bin/bash

cd ..

docker rmi vidt-gpu:latest
docker build -t vidt-gpu:latest -f ./docker/Dockerfile-gpu .
