package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import com.rabbitmq.client.GetResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.math.BigDecimal;
import java.math.RoundingMode;

import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/v1/acp")
public class AcpController {

    private final RuntimeEnvironment environment;
    private static final String STUDENT_ID = "s2305255";
    private final Gson gson = new Gson();

    public AcpController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        return props;
    }

    @PutMapping("/messages/rabbitmq/{queueName}/{messageCount}")
    public ResponseEntity<Void> writeRabbitMqMessages(
            @PathVariable String queueName,
            @PathVariable int messageCount) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(queueName, true, false, false, null);

            for (int i = 0; i < messageCount; i++) {
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", STUDENT_ID);
                msg.addProperty("counter", i);
                channel.basicPublish("", queueName, null,
                        gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok().build();
    }

    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> writeKafkaMessages(
            @PathVariable String writeTopic,
            @PathVariable int messageCount) {

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProperties())) {
            for (int i = 0; i < messageCount; i++) {
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", STUDENT_ID);
                msg.addProperty("counter", i);
                producer.send(new ProducerRecord<>(writeTopic, String.valueOf(i), gson.toJson(msg))).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok().build();
    }

    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> readRabbitMqMessages(
            @PathVariable String queueName,
            @PathVariable long timeoutInMsec) {

        List<String> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutInMsec;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(queueName, true, false, false, null);

            while (System.currentTimeMillis() < deadline) {
                GetResponse response = channel.basicGet(queueName, true);
                if (response != null) {
                    messages.add(new String(response.getBody(), StandardCharsets.UTF_8));
                } else {
                    Thread.sleep(10);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok(messages);
    }

    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> readKafkaMessages(
            @PathVariable String readTopic,
            @PathVariable long timeoutInMsec) {

        List<String> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProperties())) {
            consumer.subscribe(Collections.singletonList(readTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutInMsec));
            for (ConsumerRecord<String, String> record : records) {
                messages.add(record.value());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok(messages);
    }

    @GetMapping("/messages/sorted/rabbitmq/{queueName}/{messagesToConsider}")
    public ResponseEntity<String> readSortedRabbitMqMessages(
            @PathVariable String queueName,
            @PathVariable int messagesToConsider) {

        List<JsonObject> messages = new ArrayList<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(queueName, true, false, false, null);

            while (messages.size() < messagesToConsider) {
                GetResponse response = channel.basicGet(queueName, true);
                if (response != null) {
                    String body = new String(response.getBody(), StandardCharsets.UTF_8);
                    messages.add(gson.fromJson(body, JsonObject.class));
                } else {
                    Thread.sleep(10);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        messages.sort(Comparator.comparingInt(o -> o.get("Id").getAsInt()));

        com.google.gson.JsonArray result = new com.google.gson.JsonArray();
        for (JsonObject obj : messages) {
            result.add(obj);
        }

        return ResponseEntity.ok(result.toString());
    }

    @GetMapping("/messages/sorted/kafka/{topic}/{messagesToConsider}")
    public ResponseEntity<String> readSortedKafkaMessages(
            @PathVariable String topic,
            @PathVariable int messagesToConsider) {

        List<JsonObject> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProperties())) {
            consumer.subscribe(Collections.singletonList(topic));

            while (messages.size() < messagesToConsider) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(gson.fromJson(record.value(), JsonObject.class));
                    if (messages.size() >= messagesToConsider) break;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        messages.sort(Comparator.comparingInt(o -> o.get("Id").getAsInt()));

        com.google.gson.JsonArray result = new com.google.gson.JsonArray();
        for (JsonObject obj : messages) {
            result.add(obj);
        }

        return ResponseEntity.ok(result.toString());
    }

    @PostMapping("/splitter")
    public ResponseEntity<Void> splitter(@RequestBody Map<String, Object> body) {
        String readQueue = (String) body.get("readQueue");
        String redisHashOdd = (String) body.get("redisHashOdd");
        String redisHashEven = (String) body.get("redisHashEven");
        int messageCount = ((Number) body.get("messageCount")).intValue();

        List<JsonObject> messages = new ArrayList<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(readQueue, true, false, false, null);

            while (messages.size() < messageCount) {
                GetResponse response = channel.basicGet(readQueue, true);
                if (response != null) {
                    String msgBody = new String(response.getBody(), StandardCharsets.UTF_8);
                    messages.add(gson.fromJson(msgBody, JsonObject.class));
                } else {
                    Thread.sleep(10);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort());
             Jedis jedis = pool.getResource()) {

            long countEven = jedis.exists("count_even") ? Long.parseLong(jedis.get("count_even")) : 0L;
            long countOdd = jedis.exists("count_odd") ? Long.parseLong(jedis.get("count_odd")) : 0L;
            double sumEven = countEven > 0 && jedis.exists("average_even")
                    ? Double.parseDouble(jedis.get("average_even")) * countEven : 0.0;
            double sumOdd = countOdd > 0 && jedis.exists("average_odd")
                    ? Double.parseDouble(jedis.get("average_odd")) * countOdd : 0.0;

            if (!jedis.exists("count_even")) jedis.set("count_even", "0");
            if (!jedis.exists("count_odd")) jedis.set("count_odd", "0");
            if (!jedis.exists("average_even")) jedis.set("average_even", "0.0");
            if (!jedis.exists("average_odd")) jedis.set("average_odd", "0.0");

            for (JsonObject msg : messages) {
                int id = msg.get("Id").getAsInt();
                double value = msg.get("Value").getAsDouble();
                String msgJson = gson.toJson(msg);
                String idStr = String.valueOf(id);

                if (id % 2 == 0) {
                    jedis.hset(redisHashEven, idStr, msgJson);
                    countEven++;
                    sumEven += value;
                    double avgEven = BigDecimal.valueOf(sumEven / countEven)
                            .setScale(2, RoundingMode.HALF_UP).doubleValue();
                    jedis.set("count_even", String.valueOf(countEven));
                    jedis.set("average_even", String.valueOf(avgEven));
                } else {
                    jedis.hset(redisHashOdd, idStr, msgJson);
                    countOdd++;
                    sumOdd += value;
                    double avgOdd = BigDecimal.valueOf(sumOdd / countOdd)
                            .setScale(2, RoundingMode.HALF_UP).doubleValue();
                    jedis.set("count_odd", String.valueOf(countOdd));
                    jedis.set("average_odd", String.valueOf(avgOdd));
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping("/transformMessages")
    public ResponseEntity<Void> transformMessages(@RequestBody Map<String, Object> body) {
        String readQueue = (String) body.get("readQueue");
        String writeQueue = (String) body.get("writeQueue");
        int messageCount = ((Number) body.get("messageCount")).intValue();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection conn = factory.newConnection();
             Channel readChannel = conn.createChannel();
             Channel writeChannel = conn.createChannel();
             JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort());
             Jedis jedis = pool.getResource()) {

            readChannel.queueDeclare(readQueue, true, false, false, null);
            writeChannel.queueDeclare(writeQueue, true, false, false, null);

            int totalMessagesWritten = jedis.exists("tm_written") ? Integer.parseInt(jedis.get("tm_written")) : 0;
            int totalMessagesProcessed = jedis.exists("tm_processed") ? Integer.parseInt(jedis.get("tm_processed")) : 0;
            int totalRedisUpdates = jedis.exists("tm_redis_updates") ? Integer.parseInt(jedis.get("tm_redis_updates")) : 0;
            double totalValueWritten = jedis.exists("tm_value_written") ? Double.parseDouble(jedis.get("tm_value_written")) : 0.0;
            double totalAdded = jedis.exists("tm_added") ? Double.parseDouble(jedis.get("tm_added")) : 0.0;

            int count = 0;
            while (count < messageCount) {
                GetResponse response = readChannel.basicGet(readQueue, true);
                if (response == null) {
                    Thread.sleep(10);
                    continue;
                }

                String bodyStr = new String(response.getBody(), StandardCharsets.UTF_8);
                JsonObject msg = gson.fromJson(bodyStr, JsonObject.class);
                count++;
                totalMessagesProcessed++;

                String key = msg.get("key").getAsString();

                if ("TOMBSTONE".equals(key)) {
                    // Delete tracked keys and count each deletion as a Redis update
                    Set<String> trackedKeys = jedis.smembers("tm_tracked_keys");
                    for (String trackedKey : trackedKeys) {
                        jedis.del(trackedKey);
                        totalRedisUpdates++;
                    }
                    jedis.del("tm_tracked_keys");

                    // Increment totalMessagesWritten BEFORE building summary
                    totalMessagesWritten++;

                    JsonObject summary = new JsonObject();
                    summary.addProperty("totalMessagesWritten", totalMessagesWritten);
                    summary.addProperty("totalMessagesProcessed", totalMessagesProcessed);
                    summary.addProperty("totalRedisUpdates", totalRedisUpdates);
                    summary.addProperty("totalValueWritten", totalValueWritten);
                    summary.addProperty("totalAdded", totalAdded);

                    writeChannel.basicPublish("", writeQueue, null,
                            gson.toJson(summary).getBytes(StandardCharsets.UTF_8));

                } else {
                    int version = msg.get("version").getAsInt();
                    double value = msg.get("value").getAsDouble();

                    String storedVersionStr = jedis.get(key);
                    boolean shouldUpdate = storedVersionStr == null ||
                            Integer.parseInt(storedVersionStr) < version;

                    JsonObject outMsg = new JsonObject();
                    outMsg.addProperty("key", key);
                    outMsg.addProperty("version", version);

                    if (shouldUpdate) {
                        jedis.set(key, String.valueOf(version));
                        jedis.sadd("tm_tracked_keys", key);
                        totalRedisUpdates++;
                        double newValue = value + 10.5;
                        outMsg.addProperty("value", newValue);
                        totalValueWritten += newValue;
                        totalAdded += 10.5;
                    } else {
                        outMsg.addProperty("value", value);
                        totalValueWritten += value;
                    }

                    writeChannel.basicPublish("", writeQueue, null,
                            gson.toJson(outMsg).getBytes(StandardCharsets.UTF_8));
                    totalMessagesWritten++;
                }

                jedis.set("tm_written", String.valueOf(totalMessagesWritten));
                jedis.set("tm_processed", String.valueOf(totalMessagesProcessed));
                jedis.set("tm_redis_updates", String.valueOf(totalRedisUpdates));
                jedis.set("tm_value_written", String.valueOf(totalValueWritten));
                jedis.set("tm_added", String.valueOf(totalAdded));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok().build();
    }
}