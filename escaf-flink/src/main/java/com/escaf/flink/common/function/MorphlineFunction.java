package com.escaf.flink.common.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

/**
 * 在Flink中使用Morphline对数据进行一些ETL操作 1.规则的配置可以是支持本地化、第三方缓存等,具体使用可以根据实际情况选用
 * 
 * @author HANLIN
 *
 */
public class MorphlineFunction extends RichMapFunction<String, org.apache.avro.generic.GenericData.Record> {

	private static final long serialVersionUID = 1L;

	private ShardedJedis jedis = null;

	private String serverIp;

	private int serverPort;

	private int connectTimeout = 10000;

	private Map<String, Command> cmdMap = null;

	private String morphlineKey;

	private MorphlineContext morphlineContext = null;

	// 这个是线程安全的
	private Collector finalChild = null;

	private static final Logger log = LoggerFactory.getLogger(MorphlineFunction.class);

	public MorphlineFunction() {

	}

	@Override
	public void open(Configuration parameters) throws Exception {

		// FIXME 需要考虑保障redis连接挂掉了情况、重连情况、这里不使用Redis连接池
		jedis = new ShardedJedis(Arrays.asList(new JedisShardInfo(serverIp, serverPort, connectTimeout)));
		Map<String, String> logtypeToCmd = jedis.hgetAll(morphlineKey);

		morphlineContext = new MorphlineContext.Builder().setExceptionHandler(new FaultTolerance(false, false))
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("flink.morphlineContext")).build();

		cmdMap = new HashMap<String, Command>();
		Iterator<Entry<String, String>> it = logtypeToCmd.entrySet().iterator();

		finalChild = new Collector();
		while (it.hasNext()) {
			Entry<String, String> en = it.next();
			String logType = en.getKey();
			String cmdValue = en.getValue();

			Config config = ConfigFactory.parseString(cmdValue);

			Command cmd = new Compiler().compile(config, morphlineContext, finalChild);
			cmdMap.put(logType, cmd);
		}

	}

	@Override
	public org.apache.avro.generic.GenericData.Record map(String value) throws Exception {

		try {
			String logType = "trans_log";
			Command cmd = cmdMap.get(logType);
			if (null != cmd) {
				Record record = new Record();
				record.put(Fields.ATTACHMENT_BODY, value.getBytes("UTF-8"));

				Notifications.notifyStartSession(cmd);
				if (!cmd.process(record)) {

					log.warn("Morphline {} failed to process record: {}", value.toString());
					return null;
				}
				record = finalChild.getRecords().get(0);
				GenericData.Record resultRecord = (org.apache.avro.generic.GenericData.Record) record
						.get("_attachment_body").get(0);

				if (null == resultRecord) {
					throw new NullPointerException("handle record faild value=" + value);
				}

				return resultRecord;

			}

			return null;
		} finally {
			finalChild.reset();
		}

	}

	@Override
	public void close() throws Exception {
		if (null != jedis) {

			jedis.disconnect();

		}
		if (null != cmdMap && !cmdMap.isEmpty()) {
			for (Command cmd : cmdMap.values()) {
				Notifications.notifyShutdown(cmd);
			}

			cmdMap = null;

		}

	}

	public static MorphlineFunctionBuidler buildMorphlineFunction() {

		return new MorphlineFunctionBuidler();

	}

	public static class MorphlineFunctionBuidler {

		private final MorphlineFunction morphlineFunction;

		protected MorphlineFunctionBuidler() {
			this.morphlineFunction = new MorphlineFunction();
		}

		public MorphlineFunctionBuidler setServerIp(String serverIp) {
			morphlineFunction.serverIp = serverIp;
			return this;
		}

		public MorphlineFunctionBuidler setServerPort(int serverPort) {
			morphlineFunction.serverPort = serverPort;
			return this;
		}

		public MorphlineFunctionBuidler setConnectTimeout(int connectTimeout) {
			morphlineFunction.connectTimeout = connectTimeout;
			return this;
		}

		public MorphlineFunctionBuidler setMorphlineKey(String key) {
			morphlineFunction.morphlineKey = key;
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

	public static final class Collector implements Command {

		private final List<Record> results = new ArrayList<Record>();

		public List<Record> getRecords() {
			return results;
		}

		public void reset() {
			results.clear();
		}

		public Command getParent() {
			return null;
		}

		public void notify(Record notification) {
		}

		public boolean process(Record record) {
			Preconditions.checkNotNull(record);
			results.add(record);
			return true;
		}

	}

}
