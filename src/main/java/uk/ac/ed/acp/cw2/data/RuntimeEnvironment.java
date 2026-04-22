package uk.ac.ed.acp.cw2.data;

public class RuntimeEnvironment {

    public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
    public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
    public static final String RABBITMQ_HOST_ENV_VAR = "RABBITMQ_HOST";
    public static final String RABBITMQ_PORT_ENV_VAR = "RABBITMQ_PORT";
    public static final String KAFKA_BOOTSTRAP_SERVERS_ENV_VAR = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String KAFKA_INBOUND_TOPIC = "KAFKA_INBOUND_TOPIC";
    public static final String KAFKA_OUTBOUND_TOPIC = "KAFKA_OUTBOUND_TOPIC";
    public static final String KAFKA_SECURITY_PROTOCOL_ENV_VAR = "KAFKA_SECURITY_PROTOCOL";
    public static final String KAFKA_SASL_MECHANISM_ENV_VAR = "KAFKA_SASL_MECHANISM";
    public static final String KAFKA_SASL_JAAS_CONFIG_ENV_VAR = "KAFKA_SASL_JAAS_CONFIG";

    private String redisHost;
    private int redisPort;
    private String rabbitMqHost;
    private int rabbitMqPort;
    private String kafkaBootstrapServers;
    private String kafkaInboundTopic;
    private String kafkaOutboundTopic;
    private String kafkaSecurityProtocol;
    private String kafkaSaslMechanism;
    private String kafkaSaslJaasConfig;

    public String getRedisHost() { return redisHost; }
    public void setRedisHost(String v) { this.redisHost = v; }

    public int getRedisPort() { return redisPort; }
    public void setRedisPort(int v) { this.redisPort = v; }

    public String getRabbitMqHost() { return rabbitMqHost; }
    public void setRabbitMqHost(String v) { this.rabbitMqHost = v; }

    public int getRabbitMqPort() { return rabbitMqPort; }
    public void setRabbitMqPort(int v) { this.rabbitMqPort = v; }

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public void setKafkaBootstrapServers(String v) { this.kafkaBootstrapServers = v; }

    public String getKafkaInboundTopic() { return kafkaInboundTopic; }
    public void setKafkaInboundTopic(String v) { this.kafkaInboundTopic = v; }

    public String getKafkaOutboundTopic() { return kafkaOutboundTopic; }
    public void setKafkaOutboundTopic(String v) { this.kafkaOutboundTopic = v; }

    public String getKafkaSecurityProtocol() { return kafkaSecurityProtocol; }
    public void setKafkaSecurityProtocol(String v) { this.kafkaSecurityProtocol = v; }

    public String getKafkaSaslMechanism() { return kafkaSaslMechanism; }
    public void setKafkaSaslMechanism(String v) { this.kafkaSaslMechanism = v; }

    public String getKafkaSaslJaasConfig() { return kafkaSaslJaasConfig; }
    public void setKafkaSaslJaasConfig(String v) { this.kafkaSaslJaasConfig = v; }

    public static RuntimeEnvironment getEnvironment() {
        RuntimeEnvironment settings = new RuntimeEnvironment();

        settings.setKafkaBootstrapServers(System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR) == null ? "localhost:9092" : System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR));
        settings.setKafkaInboundTopic(System.getenv(KAFKA_INBOUND_TOPIC) == null ? "cw2-inbound" : System.getenv(KAFKA_INBOUND_TOPIC));
        settings.setKafkaOutboundTopic(System.getenv(KAFKA_OUTBOUND_TOPIC) == null ? "cw2-outbound" : System.getenv(KAFKA_OUTBOUND_TOPIC));
        settings.setRedisHost(System.getenv(REDIS_HOST_ENV_VAR) == null ? "localhost" : System.getenv(REDIS_HOST_ENV_VAR));
        settings.setRedisPort(System.getenv(REDIS_PORT_ENV_VAR) == null ? 6379 : Integer.parseInt(System.getenv(REDIS_PORT_ENV_VAR)));
        settings.setRabbitMqHost(System.getenv(RABBITMQ_HOST_ENV_VAR) == null ? "localhost" : System.getenv(RABBITMQ_HOST_ENV_VAR));
        settings.setRabbitMqPort(System.getenv(RABBITMQ_PORT_ENV_VAR) == null ? 5672 : Integer.parseInt(System.getenv(RABBITMQ_PORT_ENV_VAR)));

        if (System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR) != null) {
            if (System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR) == null || System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR) == null) {
                throw new RuntimeException("if security is set up all 3 security attributes must be specified");
            }
            settings.setKafkaSecurityProtocol(System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR));
            settings.setKafkaSaslMechanism(System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR));
            settings.setKafkaSaslJaasConfig(System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR));
        }

        return settings;
    }
}