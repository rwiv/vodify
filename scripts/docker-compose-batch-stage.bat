cd ..
docker compose -f ./docker/docker-compose-batch-stage.yml --env-file ./dev/.env-batch-stage up
pause