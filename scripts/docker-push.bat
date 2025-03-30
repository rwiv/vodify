cd ..
set IMG=harbor.rwiv.xyz/private/vtask:0.1.4
set DOCKERFILE=./docker/Dockerfile-prod

docker rmi %IMG%

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause