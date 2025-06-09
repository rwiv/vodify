cd ..
set IMG=vtask-enc:latest
set DOCKERFILE=./docker/Dockerfile-enc

docker rmi %IMG%
docker build -t %IMG% -f %DOCKERFILE% .
pause