/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.service.kafka;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Class description here.
 */
public class KafkaServiceTest {

  @Ignore
  @Test
  public void testFoo() throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
    consumer.subscribe(Lists.newArrayList("test"));
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.offset() + ": " + record.value());
        }
        break;
      }
    } finally {
      consumer.close();
    }
  }
}
