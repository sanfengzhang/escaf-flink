package com.escaf.flink.app;

import org.apache.avro.generic.GenericData.Record;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;

import com.escaf.flink.common.BaseEscafJob;
import com.escaf.flink.common.EscafFlinkSupport;
import com.escaf.flink.common.function.JDBCOutputFormat;
import com.escaf.flink.common.function.JdbcRowFunction;

public class TransApp {

	public static void main(String[] args) throws Exception {

		TransJob transJob = new TransJob();
		transJob.start();

	}
}

class TransJob extends BaseEscafJob {

	private static final String RESATRT_ATTEMPTS = "flink.restartAttempts";

	private static final String DELAY_BETWEEN_ATTEMPTS = "flink.delayBetweenAttempts";

	private static final String CHECKPOINT_INTERVAL = "flink.checkponit.interval";
	

	@SuppressWarnings("unchecked")
	public void start() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(
				flinkConfig.getInteger(RESATRT_ATTEMPTS, 4), flinkConfig.getInteger(DELAY_BETWEEN_ATTEMPTS, 10000)));
		env.enableCheckpointing(flinkConfig.getInteger(CHECKPOINT_INTERVAL, 5000));

		env.getConfig().setGlobalJobParameters(getGlobalJobParameters());

		DataStream<String> messageStream = env
				.addSource(EscafFlinkSupport.createKafkaConsumer010(new SimpleStringSchema(), flinkConfig));

		DataStream<Record> messageStream1 = messageStream.map(EscafFlinkSupport.createMorphlineFunction(flinkConfig));

		DataStream<Row> messageStream2 = messageStream1.flatMap(new JdbcRowFunction(flinkConfig));

		JDBCOutputFormat format = EscafFlinkSupport.createJDBCOutputFormatMysql(flinkConfig);
		messageStream2.addSink(new OutputFormatSinkFunction<Row>(format));

		env.execute("trans job");

	}

}
