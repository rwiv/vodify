#!/bin/sh

cd ~/project/vtask

sudo docker compose -f ./docker/docker-compose-server-prod.yml --env-file ./secret/.env-server-prod stop
sudo docker compose -f ./docker/docker-compose-server-prod.yml --env-file ./secret/.env-server-prod rm -f

sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env-worker-prod stop
sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env-worker-prod rm -f

git pull

sudo docker compose -f ./docker/docker-compose-server-prod.yml --env-file ./secret/.env-server-prod up -d

sudo docker compose -f ./docker/docker-compose-worker-prod.yml --env-file ./secret/.env-worker-prod up -d
