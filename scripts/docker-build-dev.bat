cd ..
set IMG=vodify:latest
set DOCKERFILE=./docker/Dockerfile-dev

docker rmi %IMG%
docker build -t %IMG% -f %DOCKERFILE% .
pause