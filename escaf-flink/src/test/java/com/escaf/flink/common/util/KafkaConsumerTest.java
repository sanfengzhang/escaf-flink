package com.escaf.flink.common.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaConsumerTest {

	public static void main(String[] args) throws IOException {

		Properties p = new Properties();
		p.put("bootstrap.servers", "192.168.1.100:9092");

		p.put("value.deserializer", ByteArrayDeserializer.class);
		p.put("key.deserializer", ByteArrayDeserializer.class);
		p.put("group.id", "translog-consumew2");
		p.put("enable.auto.commit", "true");
		p.put("auto.commit.interval.ms", "3000");
		p.put("session.timeout.ms", "30000");
		p.put("auto.offset.reset", "earliest");

		@SuppressWarnings("resource")
		KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(p);

		Set<String> set = new HashSet<String>();

		set.add("translog1");
		kafkaConsumer.subscribe(set);

		ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
		if (!records.isEmpty()) {
			for (ConsumerRecord<byte[], byte[]> record : records) {
				System.out.println(new String(record.value(), "UTF-8"));
			}
		}

		System.in.read();

	}

}
