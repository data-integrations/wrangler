package co.cask.wrangler.service.kafka;

import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Class description here.
 */
public final class KafkaConfiguration {
  private final boolean isZookeeperConnection;
  private final String connection;
  private final String topic;
  private final Properties props;

  public KafkaConfiguration(Connection conn) {
    String keyDeserializer = StringDeserializer.class.getName();
    String valueDeserializer = keyDeserializer;

    if (conn.getType() != ConnectionType.KAFKA) {
      throw new IllegalArgumentException(
        String.format("Connection id '%s', name '%s' is not a Kafka configuration.", conn.getId(), conn.getName())
      );
    }

    Map<String, Object> properties = conn.getAllProps();
    if(properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("Kafka properties are not defined. Check connection setting.");
    }

    isZookeeperConnection = properties.containsKey("zookeeper_quorum") ? true : false;
    if (isZookeeperConnection) {
      connection = (String) properties.get("zookeeper_quorum");
    } else {
      if(properties.containsKey("brokers")) {
        connection = (String) properties.get("brokers");
      } else {
        connection = null;
      }
    }

    if(properties.containsKey("topic")) {
      topic = (String) properties.get("topic");
    } else {
      topic = null;
    }

    if(properties.containsKey("key.type")) {
      keyDeserializer = deserialize((String) properties.get("key.type"));
    }

    if(properties.containsKey("value.type")) {
      valueDeserializer = deserialize((String) properties.get("value.type"));
    }

    props = new Properties();
    if (!isZookeeperConnection) {
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
      props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    }
  }

  private String deserialize(String type) {
    type = type.toLowerCase();
    switch(type) {
      case "string":
        return StringDeserializer.class.getName();

      case "int":
        return IntegerDeserializer.class.getName();

      case "long":
        return LongDeserializer.class.getName();

      case "double":
        return DoubleDeserializer.class.getName();

      case "bytes":
        return ByteArrayDeserializer.class.getName();

      default:
        throw new IllegalArgumentException(
          String.format("Deserializer '%s' type not supported.", type)
        );
    }
  }

  public boolean isZookeeper() {
    return isZookeeperConnection;
  }

  public String getConnection() {
    return connection;
  }

  public String getTopic() {
    return topic;
  }

  public Properties get() {
    return props;
  }
}
