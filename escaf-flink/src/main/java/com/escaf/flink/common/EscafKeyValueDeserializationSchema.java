package com.escaf.flink.common;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * 定义一个Tuple3的返回结果、包含数据的topic或者key可配置
 * 
 * @author HANLIN
 *
 */
public class EscafKeyValueDeserializationSchema implements KeyedDeserializationSchema<EscafKafkaMessage> {

	private static final long serialVersionUID = 1L;

	private final SimpleStringSchema simpleStringSchema;

	public EscafKeyValueDeserializationSchema() {

		simpleStringSchema = new SimpleStringSchema(Charset.forName("UTF-8"));

	}

	@Override
	public EscafKafkaMessage deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
			throws IOException {

		EscafKafkaMessage result = new EscafKafkaMessage();
		if (null != messageKey) {
			result.setKey(simpleStringSchema.deserialize(messageKey));
		}
		if (null != message) {
			result.setValue(simpleStringSchema.deserialize(message));
		}

		result.setTopic(topic);
		result.setPartition(partition);

		return result;
	}

	@Override
	public boolean isEndOfStream(EscafKafkaMessage nextElement) {

		return false;
	}

	@Override
	public TypeInformation<EscafKafkaMessage> getProducedType() {

		return getForClass(EscafKafkaMessage.class);
	}

}
