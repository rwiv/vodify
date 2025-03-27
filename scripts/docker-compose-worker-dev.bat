cd ..
docker compose -f ./docker/docker-compose-worker-dev1.yml --env-file ./dev/.env-worker-dev up
pause