#!/bin/bash

echo "=== Flushing Redis ==="
docker exec acpcw2template-redis-1 redis-cli FLUSHALL

echo "=== Creating RabbitMQ Queues ==="

# For endpoints 1,2 (write tests - no setup needed, just create queues)
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/testQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

# For endpoints 3,4 (read tests)
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/readTestQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

# For endpoints 5,6 (sorted tests)
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/sortedTestQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

# For endpoint 7 (splitter)
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/splitterReadQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

# For endpoint 8 (transformMessages)
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/transformReadQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/transformWriteQueue \
  -H "Content-Type: application/json" \
  -d '{"durable":true}'

echo "=== Seeding Sorted Rabbit MQ Queue (out of order) ==="
curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"sortedTestQueue","payload":"{\"Id\":3,\"Payload\":\"three\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"sortedTestQueue","payload":"{\"Id\":1,\"Payload\":\"one\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"sortedTestQueue","payload":"{\"Id\":2,\"Payload\":\"two\"}","payload_encoding":"string"}'


echo "=== Seeding Sorted Kafka Topic ==="
curl -v -X PUT http://localhost:8080/api/v1/acp/test/seed/kafka/sortedTestTopic

echo "=== Seeding Splitter Queue ==="
curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"splitterReadQueue","payload":"{\"Id\":1,\"Value\":0.5,\"AdditionalData\":\"one\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"splitterReadQueue","payload":"{\"Id\":2,\"Value\":1.5,\"AdditionalData\":\"two\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"splitterReadQueue","payload":"{\"Id\":3,\"Value\":2.0,\"AdditionalData\":\"three\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"splitterReadQueue","payload":"{\"Id\":4,\"Value\":3.0,\"AdditionalData\":\"four\"}","payload_encoding":"string"}'

echo "=== Seeding Transform Queue ==="
curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"ABC\",\"version\":1,\"value\":100}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"ABC\",\"version\":1,\"value\":200}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"ABC\",\"version\":3,\"value\":400}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"ABC\",\"version\":2,\"value\":200}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"TOMBSTONE\"}","payload_encoding":"string"}'

curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F//publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"transformReadQueue","payload":"{\"key\":\"ABC\",\"version\":2,\"value\":200}","payload_encoding":"string"}'

echo ""
echo "=== Done! All test data seeded ==="