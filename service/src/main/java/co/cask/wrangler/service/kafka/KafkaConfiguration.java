/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.service.kafka;

import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.service.connections.ConnectionType;
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
 * A class for managing configurations of Kafka.
 */
public final class KafkaConfiguration {
  private final String connection;
  private final Properties props;

  private String keyDeserializer;
  private String valueDeserializer;

  public KafkaConfiguration(Connection conn) {
    keyDeserializer = StringDeserializer.class.getName();
    valueDeserializer = ByteArrayDeserializer.class.getName();

    if (conn.getType() != ConnectionType.KAFKA) {
      throw new IllegalArgumentException(
        String.format("Connection id '%s', name '%s' is not a Kafka configuration.", conn.getId(), conn.getName())
      );
    }

    Map<String, Object> properties = conn.getAllProps();
    if(properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("Kafka properties are not defined. Check connection setting.");
    }

    if(properties.containsKey("brokers")) {
      connection = (String) properties.get("brokers");
    } else {
      throw new IllegalArgumentException("Kafka Brokers not defined.");
    }

    if(properties.containsKey("key.type")) {
      keyDeserializer = deserialize((String) properties.get("key.type"));
    }

    if(properties.containsKey("value.type")) {
      valueDeserializer = deserialize((String) properties.get("value.type"));
    }

    props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "true");
  }

  /**
   * @return String representation of key deserializer of kafka topic.
   */
  public String getKeyDeserializer() {
    return keyDeserializer;
  }

  /**
   * @return String representation of value deserializer of kafka topic.
   */
  public String getValueDeserializer() {
    return valueDeserializer;
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

  /**
   * @return connection information of kafka.
   */
  public String getConnection() {
    return connection;
  }

  /**
   * @return Kafka connection property.
   */
  public Properties get() {
    return props;
  }
}
