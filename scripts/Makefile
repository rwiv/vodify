argo-run:
	kubectl create -f ../kube/argocd/application.yaml


worker-up:
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env rm
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env up -d

worker-rm:
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env stop
	sudo docker compose -f ../docker/docker-compose-worker-prod.yml --env-file ../secret/.env rm -f

worker-local-logs:
	sudo docker logs vtask-worker-local --tail 20

worker-remote-logs:
	sudo docker logs vtask-worker-remote --tail 20


helm-redis-dep:
	@helm dependency update ./redis

helm-redis-run:
	helm install vtask-redis ../kube/redis -n media

helm-redis-rm:
	helm uninstall vtask-redis -n media

delete-redis-pvc:
	kubectl delete pvc redis-data-vtask-redis-master-0 -n media
