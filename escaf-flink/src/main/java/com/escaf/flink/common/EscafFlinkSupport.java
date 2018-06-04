package com.escaf.flink.common;

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.escaf.flink.common.function.JDBCOutputFormat;
import com.escaf.flink.common.function.MorphlineFunction;
import com.escaf.flink.common.function.RedisMorphlineFunction;

public class EscafFlinkSupport {

	private static final String KAFAK_CONSUME_TOPIC = "flink.kafka.consume.topic";

	private static final String KAFKA_BROKER_SERVERS = "flink.bootstrap.servers";

	private static final String KAFKA_CONSUME_GROUP_ID = "flink.group.id";

	public static final String JDBC_OUTPUTFORMAT_COLUMNNAME = "flink.jdbcoutputformat.cloumnname";

	private static final String JDBC_OUTPUTFORMAT_TYPES = "flink.jdbcoutputformat.types";

	private static final String JDBC_OUTPUTFORMAT_TABLENAME = "flink.jdbcoutputformat.tablename";
	
	public static final String RESATRT_ATTEMPTS = "flink.restartAttempts";

	public static final String DELAY_BETWEEN_ATTEMPTS = "flink.delayBetweenAttempts";

	public static final String CHECKPOINT_INTERVAL = "flink.checkponit.interval";

	/**
	 * 初始化kafka consumer还可以设置 // auto.offset.reset该字段的配置可以通过方法配置
	 * consumer010.setStartFromEarliest(); 这些参数、包括设置初始化offset位置
	 * 
	 * @param deserializationSchema
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static FlinkKafkaConsumer010 createKafkaConsumer010(DeserializationSchema<?> deserializationSchema,
			AbstractFlinkConfig flinkConfig) {
		Properties p = new Properties();
		p.put("bootstrap.servers", flinkConfig.getRequried(KAFKA_BROKER_SERVERS));
		p.put("group.id", flinkConfig.getRequried(KAFKA_CONSUME_GROUP_ID));

		String topics = flinkConfig.getRequried(KAFAK_CONSUME_TOPIC);
		String[] topicArrays = StringUtils.split(topics, ",");
		FlinkKafkaConsumer010 consumer010 = new FlinkKafkaConsumer010(Arrays.asList(topicArrays), deserializationSchema,
				p);
		// consumer010.setStartFromLatest();
		consumer010.setStartFromEarliest();

		return consumer010;
	}

	/**
	 * 返回JSON类型的数据包含元数据topic
	 * 
	 * @param deserializer
	 * @param flinkConfig
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static FlinkKafkaConsumer010 createKafkaConsumer010JSON(boolean includeMetadata,
			AbstractFlinkConfig flinkConfig) {

		Properties p = new Properties();
		p.put("bootstrap.servers", flinkConfig.getRequried(KAFKA_BROKER_SERVERS));
		p.put("group.id", flinkConfig.getRequried(KAFKA_CONSUME_GROUP_ID));

		String topics = flinkConfig.getRequried(KAFAK_CONSUME_TOPIC);
		String[] topicArrays = StringUtils.split(topics, ",");
		FlinkKafkaConsumer010 consumer010 = new FlinkKafkaConsumer010(Arrays.asList(topicArrays),
				new EscafKeyValueDeserializationSchema(), p);
		// consumer010.setStartFromLatest();
		consumer010.setStartFromEarliest();

		return consumer010;
	}

	@SuppressWarnings("rawtypes")
	public static FlinkKafkaProducer010 createKafkaProducer010() {
		return null;
	}

	/**
	 * create mysql OutPutFormat
	 * 
	 * @param flinkConfig
	 * @param sql
	 * @param typesArray
	 * @return
	 */
	public static JDBCOutputFormat createJDBCOutputFormatMysql(AbstractFlinkConfig flinkConfig) {

		String tablename = flinkConfig.getRequried(JDBC_OUTPUTFORMAT_TABLENAME);
		String columns = flinkConfig.getRequried(JDBC_OUTPUTFORMAT_COLUMNNAME);

		StringBuilder sqlBuilder = new StringBuilder("insert into ");
		sqlBuilder.append(tablename);
		sqlBuilder.append(" (").append(columns).append(") values (");

		String arrayColumns[] = columns.split(",");
		int lenarrayColumns = arrayColumns.length;
		for (int i = 0; i < lenarrayColumns; i++) {
			if (i == lenarrayColumns - 1) {
				sqlBuilder.append("?");
			} else {
				sqlBuilder.append("?,");
			}

		}

		sqlBuilder.append(")");

		String[] typesArrayStr = flinkConfig.getRequried(JDBC_OUTPUTFORMAT_TYPES).split(",");
		int len = typesArrayStr.length;
		int typesArray[] = new int[len];
		for (int i = 0; i < len; i++) {
			int type = Integer.parseInt(typesArrayStr[i]);
			typesArray[i] = type;
		}

		return JDBCOutputFormat.buildJDBCOutputFormat().setDrivername("com.mysql.jdbc.Driver")
				.setDBUrl(flinkConfig.getRequried("flink.sink.dburl"))
				.setUsername(flinkConfig.getRequried("flink.jdbc.username"))
				.setPassword(flinkConfig.getRequried("flink.jdbc.passwd")).setQuery(sqlBuilder.toString())
				.setSqlTypes(typesArray).setBatchInterval(1000).finish();
	}

	public static MorphlineFunction createRedisMorphlineFunction(AbstractFlinkConfig flinkConfig) {

		return RedisMorphlineFunction.buildMorphlineFunction().setServerPort(flinkConfig.getInteger("flink.redis.port"))
				.setConnectTimeout(flinkConfig.getInteger("flink.redis.timeout"))
				.setServerIp(flinkConfig.getRequried("flink.redis.host"))
				.setMorphlineKey(flinkConfig.getRequried("flink.redis.morphlinekey"))
				.setDataType(flinkConfig.get("flink.morphline.datatype", null)).build();

	}

}
