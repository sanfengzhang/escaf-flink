package com.escaf.flink.common.function;

import java.util.ArrayList;
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

/**
 * 在Flink中使用Morphline对数据进行一些ETL操作 1.规则的配置可以是支持本地化、第三方缓存等,具体使用可以根据实际情况选用
 * 
 * @author HANLIN
 *
 */
public abstract class MorphlineFunction extends RichMapFunction<String, org.apache.avro.generic.GenericData.Record> {

	private static final long serialVersionUID = 1L;

	private Map<String, Command> cmdMap = null;

	private MorphlineContext morphlineContext = null;

	// 这个是线程安全的
	private Collector finalChild = null;
	
	protected String commandDataType;
	
	
	private static final Logger log = LoggerFactory.getLogger(MorphlineFunction.class);

	public MorphlineFunction() {

	}

	protected abstract Map<String, String> getCommandString();

	@Override
	public void open(Configuration parameters) throws Exception {

		Map<String, String> logtypeToCmd = getCommandString();

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
						
			Command cmd = cmdMap.get(commandDataType);
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

		if (null != cmdMap && !cmdMap.isEmpty()) {
			for (Command cmd : cmdMap.values()) {
				Notifications.notifyShutdown(cmd);
			}

			cmdMap = null;

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
