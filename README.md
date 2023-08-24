Docker Commands:

docker network ls
docker inspect stream_network
docker network create stream_network
docker run -d --name rabbitmq_container --network stream_network -p 15672:15672 -p 5672:5672 rabbitmq:3-management

docker run -d --name stream_ingest_cont --network stream_network -p 8000:8000 stream_ingest_image

docker build -t stream_ingest_image .