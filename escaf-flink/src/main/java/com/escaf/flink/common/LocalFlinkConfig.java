package com.escaf.flink.common;

public class LocalFlinkConfig extends AbstractFlinkConfig {

	public LocalFlinkConfig() {
		// TODO 可以初始化一些操作eg:从资源文件中读取配置到本地缓存中来
		cacheConfig.put("flink.bootstrap.servers", "192.168.1.100:9092");
		cacheConfig.put("flink.kafka.consume.topic", "translog1");
		cacheConfig.put("flink.group.id", "translog-consumer-group1");

		cacheConfig.put("flink.sink.dburl", "jdbc:mysql://localhost:3306/han?useUnicode=true&characterEncoding=UTF-8");
		cacheConfig.put("flink.jdbc.username", "root");
		cacheConfig.put("flink.jdbc.passwd", "1234");
		cacheConfig.put("flink.jdbcoutputformat.cloumnname",
				"trans_date,trans_code,trans_channel_id,trans_start_datetime,trans_end_datetime,trans_cust_time,trans_org_id,trans_clerk,"
						+ "trans_return_code,trans_err_msg,trans_tuexdo_name");

		cacheConfig.put("flink.jdbcoutputformat.tablename", "t2_trans_log");
		cacheConfig.put("flink.jdbcoutputformat.types", "91,12,12,93,93,8,12,12,12,12,12");

		cacheConfig.put("flink.redis.host", "192.168.1.40");
		cacheConfig.put("flink.redis.port", "6379");
		cacheConfig.put("flink.redis.timeout", "100000");
		
		cacheConfig.put("flink.redis.morphlinekey", "Morphline");

	}

	@Override
	public String get(String key) {

		return cacheConfig.get(key);
	}

	@Override
	public String get(String key, String defaultValue) {
		String value = get(key);
		if (null == value || value.trim().equals("")) {
			return defaultValue;
		}

		return value;
	}

	@Override
	public String getRequried(String key) {
		String value = get(key);
		if (null != value) {
			return value;
		}
		throw new NullPointerException("requried key");
	}

}
