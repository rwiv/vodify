#!/bin/sh

cd ..

sudo docker compose -f ./docker/docker-compose-batch-encoding.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-batch-encoding.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-batch-encoding.yml --env-file ./secret/.env up -d
