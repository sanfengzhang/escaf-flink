package com.escaf.flink.common.function;

import java.util.Arrays;
import java.util.Map;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

public class RedisMorphlineFunction extends MorphlineFunction {

	private static final long serialVersionUID = 1L;

	private ShardedJedis jedis = null;

	private String serverIp;

	private int serverPort;

	private int connectTimeout = 10000;

	private String morphlineKey;

	@Override
	protected Map<String, String> getMorphlineCommandsMap() {
		// FIXME 需要考虑保障redis连接挂掉了情况、重连情况、这里不使用Redis连接池
		jedis = new ShardedJedis(Arrays.asList(new JedisShardInfo(serverIp, serverPort, connectTimeout)));		
		Map<String, String> logtypeToCmd = jedis.hgetAll(morphlineKey);

		return logtypeToCmd;
	}

	@Override
	public void close() throws Exception {

		super.close();
		if (null != jedis) {
			jedis.close();
		}
	}

	public static RedisMorphlineFunctionBuidler buildMorphlineFunction() {

		return new RedisMorphlineFunctionBuidler();

	}

	public static class RedisMorphlineFunctionBuidler {

		private final RedisMorphlineFunction morphlineFunction;

		protected RedisMorphlineFunctionBuidler() {
			this.morphlineFunction = new RedisMorphlineFunction();
		}

		public RedisMorphlineFunctionBuidler setServerIp(String serverIp) {
			morphlineFunction.serverIp = serverIp;
			return this;
		}

		public RedisMorphlineFunctionBuidler setServerPort(int serverPort) {
			morphlineFunction.serverPort = serverPort;
			return this;
		}

		public RedisMorphlineFunctionBuidler setConnectTimeout(int connectTimeout) {
			morphlineFunction.connectTimeout = connectTimeout;
			return this;
		}

		public RedisMorphlineFunctionBuidler setMorphlineKey(String key) {
			morphlineFunction.morphlineKey = key;
			return this;
		}

		public RedisMorphlineFunctionBuidler setDataType(String dataType) {
			morphlineFunction.dataType = dataType;
			return this;
		}

		public MorphlineFunction build() {

			if (morphlineFunction.serverIp == null) {
				throw new IllegalArgumentException("No Redis URL supplied.");
			}
			if (morphlineFunction.serverPort <= 0) {
				throw new IllegalArgumentException("No Redis port suplied");
			}

			return morphlineFunction;

		}

	}

}
