#!/bin/sh

cd ..

sudo docker compose -f ./docker/docker-compose-batch-loss.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-batch-loss.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-batch-loss.yml --env-file ./secret/.env up -d
