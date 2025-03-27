cd ..
docker compose -f ./docker/docker-compose-worker-dev.yml --env-file ./dev/.env-worker-dev up
pause