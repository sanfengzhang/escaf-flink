package com.escaf.flink.app;

import java.util.Map;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import com.escaf.flink.common.BaseEscafJob;
import com.escaf.flink.common.EscafFlinkSupport;
import com.escaf.flink.common.EscafKafkaMessage;
import com.escaf.flink.common.function.JDBCOutputFormat;
import com.escaf.flink.common.function.JdbcRowFunction;

public class TransApp {

	public static void main(String[] args) throws Exception {

		TransJob transJob = new TransJob();
		transJob.start();

	}
}

class TransJob extends BaseEscafJob {

	@SuppressWarnings("unchecked")
	public void start() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig()
				.setRestartStrategy(RestartStrategies.fixedDelayRestart(
						flinkConfig.getInteger(EscafFlinkSupport.RESATRT_ATTEMPTS, 4),
						flinkConfig.getInteger(EscafFlinkSupport.DELAY_BETWEEN_ATTEMPTS, 10000)));
		env.enableCheckpointing(flinkConfig.getInteger(EscafFlinkSupport.CHECKPOINT_INTERVAL, 5000));

		env.getConfig().setGlobalJobParameters(getGlobalJobParameters());

		DataStream<EscafKafkaMessage> messageStream = env
				.addSource(EscafFlinkSupport.createKafkaConsumer010JSON(true, flinkConfig));

		DataStream<Map<String,Object>> messageStream1 = messageStream
				.map(EscafFlinkSupport.createRedisMorphlineFunction(flinkConfig));

		DataStream<Row> messageStream2 = messageStream1.flatMap(new JdbcRowFunction(flinkConfig));

		JDBCOutputFormat format = EscafFlinkSupport.createJDBCOutputFormatMysql(flinkConfig);
		messageStream2.addSink(new OutputFormatSinkFunction<Row>(format));

		env.execute("trans job");

	}

}
