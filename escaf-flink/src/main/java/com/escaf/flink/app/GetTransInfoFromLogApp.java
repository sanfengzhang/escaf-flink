package com.escaf.flink.app;

import java.util.Map;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.escaf.flink.app.transinfotask.GetTransInfoFilterFunction;
import com.escaf.flink.app.transinfotask.TradeInfoSplitSelector;
import com.escaf.flink.app.transinfotask.TradePatternSelectFunction;
import com.escaf.flink.app.transinfotask.TransSimpleCondition;
import com.escaf.flink.common.BaseEscafJob;
import com.escaf.flink.common.EscafFlinkSupport;
import com.escaf.flink.common.EscafKafkaMessage;

/**
 * 需求背景：有很多IP设备的交易日志记录，其中日志记录的方式只有在五条指定规则日志连续出现才 可以知道该交易是否是成功、并且可以从中取出该次交易的信息.
 * 设计过程： 在采集端日志采集的时候需要将设备IP、日志发生时间进行丰富处理
 * 采集完毕之后日志进入到kafka、因为是同一种类型的日志、但对某一IP设备的日志需要保证时序的,如果针对每一个IP的日志
 * 创建一个Topic、在一定程度上有所浪费。
 * 针对日志类型相同有多个不同的来源、我们创建一个topic、可以有多个分区、也不用每一个分区对应一个IP的日志、采用
 * 一个分区对应多个IP的日志、(因为该业务场景每个IP的日志不是非常密集、巨大的)
 * 这种场景下、如果日志不做任何改变只是根据连续行去做处理、难度比较大、考虑的问题比较多、这样任务只能有一个并行度去处理
 * 日志，在处理流程上严格保证一份日志只能有一个线程去处理。 因为分布式多线程任务去处理、带来的问题是:你需要考虑数据顺序出现的各种情况、
 * 
 * 1.那么在不改变日志的情况下尝试去用单个并行度去处理
 * 2.可以在日志记录上做一些改进,在表示是一笔记录的日志上加一个唯一表示UUID，这样对日志的统计计算有很大的帮助
 * 
 * 
 *
 * @author HANLIN
 *
 */
@SuppressWarnings("deprecation")
public class GetTransInfoFromLogApp {

	public static void main(String[] args) throws Exception {
		GetTransInfoFromLogJob job = new GetTransInfoFromLogJob("local.flink2.properties");
		job.start();

	}

}

class GetTransInfoFromLogJob extends BaseEscafJob {

	public GetTransInfoFromLogJob(String resourceFileName) {
		super(resourceFileName);
	}

	@Override
	public void start() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();
		env.getConfig()
				.setRestartStrategy(RestartStrategies.fixedDelayRestart(
						flinkConfig.getInteger(EscafFlinkSupport.RESATRT_ATTEMPTS, 1),
						flinkConfig.getInteger(EscafFlinkSupport.DELAY_BETWEEN_ATTEMPTS, 10000)));
		env.enableCheckpointing(flinkConfig.getInteger(EscafFlinkSupport.CHECKPOINT_INTERVAL, 5000));

		env.getConfig().setGlobalJobParameters(getGlobalJobParameters());

		@SuppressWarnings("unchecked")
		DataStream<EscafKafkaMessage> messageStream = env
				.addSource(EscafFlinkSupport.createKafkaConsumer010JSON(true, flinkConfig));

		String[] transIps = new String[] { "10.1.75.181" };
		DataStream<Map<String, Object>> messageStream1 = messageStream
				.map(EscafFlinkSupport.createRedisMorphlineFunction(flinkConfig));
		@SuppressWarnings("deprecation")
		DataStream<Map<String, Object>> splitStream = messageStream1.filter(new GetTransInfoFilterFunction());

		SplitStream<Map<String, Object>> splitStream1 = splitStream.split(new TradeInfoSplitSelector(transIps));

		Pattern<Map<String, Object>, Map<String, Object>> pattern = Pattern.<Map<String, Object>>begin("start")
				.where(new TransSimpleCondition(234)).next("request").where(new TransSimpleCondition(190)).next("call")
				.where(new TransSimpleCondition(406)).next("response").where(new TransSimpleCondition(204)).next("end")
				.where(new TransSimpleCondition(423)).within(Time.seconds(90));

		PatternStream<Map<String, Object>> patternStream = CEP.pattern(splitStream1, pattern);

		DataStream<String> de = patternStream.select(new TradePatternSelectFunction());
		de.print();
		env.execute("get trans log");
	}

}
