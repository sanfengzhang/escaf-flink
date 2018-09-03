package com.escaf.flink.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import com.escaf.flink.common.BaseEscafJob;
import com.escaf.flink.common.EscafFlinkSupport;
import com.escaf.flink.common.function.JDBCOutputFormat;
import com.escaf.flink.common.function.JdbcRowFunction;

public class WeiboApp {

	public static void main(String[] args) {

		WeiboAppJob weiboAppJob = new WeiboAppJob("local.weibo.properties");
		try {
			weiboAppJob.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

class WeiboAppJob extends BaseEscafJob {

	public WeiboAppJob(String resourceFileName) {
		super(resourceFileName);
	}

	@Override
	public void start() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig()
				.setRestartStrategy(RestartStrategies.fixedDelayRestart(
						flinkConfig.getInteger(EscafFlinkSupport.RESATRT_ATTEMPTS, 4),
						flinkConfig.getInteger(EscafFlinkSupport.DELAY_BETWEEN_ATTEMPTS, 10000)));
		env.enableCheckpointing(flinkConfig.getInteger(EscafFlinkSupport.CHECKPOINT_INTERVAL, 5000));

		env.getConfig().setGlobalJobParameters(getGlobalJobParameters());

		env.setParallelism(1);

		List<String> list = IOUtils.readLines(
				new FileInputStream(
						new File("F:\\data\\Weibo Data\\Weibo Data\\weibo_train_data(new)\\weibo_train_data.txt")),
				"UTF-8");

		DataStream<String> dataStream = env.fromCollection(list);

		DataStream<Map<String, Object>> dataRowStream = dataStream.map(new DataAnalysisFunction());

		DataStream<Row> rowStream = dataRowStream.flatMap(new JdbcRowFunction(flinkConfig));

		JDBCOutputFormat format = EscafFlinkSupport.createJDBCOutputFormatMysql(flinkConfig);
		rowStream.addSink(new OutputFormatSinkFunction<Row>(format));

		env.execute("execute weibo job");

	}

}

class DataAnalysisFunction extends RichMapFunction<String, Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	private FileOutputStream fos = null;

	@Override
	public Map<String, Object> map(String value) throws Exception {
		String values[] = value.split("	", 7);

		Map<String, Object> resultMap = new java.util.HashMap<String, Object>(6);
		resultMap.put("uid", values[0]);
		resultMap.put("mid", values[1]);
		resultMap.put("time", values[2]);
		resultMap.put("forward_count", values[3]);
		resultMap.put("comment_count", values[4]);
		resultMap.put("like_count", values[5]);

		try {
			IOUtils.write(values[6], fos, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}

		return resultMap;
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		super.open(parameters);

		fos = new FileOutputStream(new File("C:\\Users\\owner\\Desktop\\weibodata.txt"));
	}

	@Override
	public void close() throws Exception {

		super.close();

		if (null != fos) {
			IOUtils.closeQuietly(fos);
		}
	}

}
