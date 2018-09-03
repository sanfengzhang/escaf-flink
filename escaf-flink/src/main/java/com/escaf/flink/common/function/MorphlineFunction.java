package com.escaf.flink.common.function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.escaf.flink.common.EscafKafkaMessage;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 在Flink中使用Morphline对数据进行一些ETL操作 1.规则的配置可以是支持本地化、第三方缓存等,具体使用可以根据实际情况选用
 * 怎么设计合理呢？有三种方式： 1.每种日志对应一个数据解析的任务，也就是一个job只负责一种类型的日志解析(有点浪费资源)
 * 2.一个解析任务可以支持多种类型数据解析、但是要求有标志日志类型的字段
 * 3.对任意一条数据使用所有的Command进行解析匹配输出结果、这种情况存在这样的一种状况就是：
 * 某条数据可以在两个或两个以上的Command解析成功(在存在这种结果下可能产生脏数据、并且数据类型多了、对性能浪费存在一定的开销)
 * 根据实际情况设计为：一种日志类型对应Kafka的一个Topic、对应的cmdMap的key值就是日志类型、这样去设计、一个任务可以消费多个topic的数据
 * 同时取到数据对应的Topic信息，这样就可以知道消息对应的日志类型从而选择相应的Command
 * 
 * @author HANLIN
 *
 */
public abstract class MorphlineFunction extends RichMapFunction<EscafKafkaMessage, Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	private Map<String, Command> cmdMap = null;

	private MorphlineContext morphlineContext = null;

	// 这个是线程安全的
	private Collector finalChild = null;

	protected String dataType = null;

	private static final Logger log = LoggerFactory.getLogger(MorphlineFunction.class);

	public MorphlineFunction() {

	}

	protected abstract Map<String, String> getMorphlineCommandsMap();

	@Override
	public void open(Configuration parameters) throws Exception {

		Map<String, String> logtypeToCmd = getMorphlineCommandsMap();

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
	public Map<String, Object> map(EscafKafkaMessage message) throws Exception {

		try {

			Command cmd = null;
			if (null == message.getTopic()) {
				cmd = cmdMap.get(dataType);
			} else {
				cmd = cmdMap.get(message.getTopic());
			}
			if (null != cmd) {
				Record record = new Record();
				record.put(Fields.ATTACHMENT_BODY, message.getValue().getBytes("UTF-8"));

				Notifications.notifyStartSession(cmd);
				if (!cmd.process(record)) {

					log.warn("Morphline {} failed to process record: {}", message.toString());
					return null;
				}
				record = finalChild.getRecords().get(0);

				Map<String, Collection<Object>> list = record.getFields().asMap();
				Iterator<Entry<String, Collection<Object>>> it = list.entrySet().iterator();

				Map<String, Object> resultMap = new HashMap<String, Object>();
				while (it.hasNext()) {
					Entry<String, Collection<Object>> en = it.next();

					if (en.getKey().equals("message")) {
						continue;
					}

					resultMap.put(en.getKey(), en.getValue().iterator().next());
				}

				return resultMap;

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
