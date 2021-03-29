### Use kafka with the docker compose
- Run container
```
docker-compose up -d
```
- Stop container
```
docker-compose down
```
- Create a topic
```
kafka-topics.sh --create --zookeeper localhost:2181 --topic  topic1 \
--partitions 1 \
--replication-factor 1
```
