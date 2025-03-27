cd ..
set IMG=ghcr.io/rwiv/vtask:0.3.2
set DOCKERFILE=./docker/Dockerfile-prod

docker rmi %IMG%

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause