cd ..
set IMG=harbor.rwiv.xyz/private/vtask:0.7.8
set DOCKERFILE=./docker/Dockerfile-prod

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause