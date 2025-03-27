#!/bin/sh

cd ~/project/vtask

sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env rm -f

git pull

sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env up -d