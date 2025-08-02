cd ..
set IMG=vidt:latest
set DOCKERFILE=./docker/Dockerfile-dev

docker rmi %IMG%
docker build -t %IMG% -f %DOCKERFILE% .
pause