package com.escaf.flink.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

public class KafakProducer {

	@Test
	public void createTradeLog() throws FileNotFoundException, IOException {

		Properties props = getProperties();

		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

		List<String> _list = IOUtils
				.readLines(new FileInputStream(new File("C:\\Users\\owner\\Desktop\\10.1.75.181.log")), "UTF-8");

		for (String eventStr : _list) {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("trans_trade_topic1",
					eventStr.getBytes("UTF-8"));
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
					System.out.println(recordMetadata.offset());

				}
			});

		}

		producer.close();

	}

	@Test
	public void createLog() throws FileNotFoundException, IOException {

		Properties props = getProperties();

		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

		List<String> _list = IOUtils.readLines(new FileInputStream(new File("C:\\Users\\owner\\Desktop\\data.txt")),
				"UTF-8");

		for (String eventStr : _list) {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("trans_log_test",
					eventStr.getBytes("UTF-8"));
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
					System.out.println(recordMetadata.offset());

				}
			});

		}

		producer.close();

	}

	private Properties getProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.100:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", ByteArraySerializer.class);
		props.put("value.serializer", ByteArraySerializer.class);

		return props;
	}
}
