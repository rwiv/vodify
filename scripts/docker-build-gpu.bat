cd ..
set IMG=vidt-gpu:latest
set DOCKERFILE=./docker/Dockerfile-gpu

docker rmi %IMG%
docker build -t %IMG% -f %DOCKERFILE% .
pause