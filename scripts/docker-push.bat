cd ..
set IMG=harbor.rwiv.xyz/private/vodify:0.9.3
set DOCKERFILE=./docker/Dockerfile-prod

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause