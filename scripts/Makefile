server-up:
	sudo docker compose -f ../docker/docker-compose-server-prod.yml --env-file ../secret/.env-server-prod rm
	sudo docker compose -f ../docker/docker-compose-server-prod.yml --env-file ../secret/.env-server-prod up -d

server-rm:
	sudo docker compose -f ../docker/docker-compose-server-prod.yml --env-file ../secret/.env-server-prod stop
	sudo docker compose -f ../docker/docker-compose-server-prod.yml --env-file ../secret/.env-server-prod rm -f

server-logs:
	sudo docker logs vtask-server --tail 20

worker-up:
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env-worker-prod rm
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env-worker-prod up -d

worker-rm:
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env-worker-prod stop
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env-worker-prod rm -f

worker-local-logs:
	sudo docker logs vtask-worker-local --tail 20

worker-remote-logs:
	sudo docker logs vtask-worker-remote --tail 20
