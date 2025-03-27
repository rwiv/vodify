cd ..
docker compose -f ./docker/docker-compose-server-dev.yml --env-file ./dev/.env-server-dev up
pause