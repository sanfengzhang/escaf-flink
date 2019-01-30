package com.douyu.omdap.dataanalysis;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.util.UpdatingPlanChecker;
import org.apache.flink.types.Row;
import org.springframework.core.annotation.AnnotationUtils;

import com.douyu.omdap.common.AbstractFlinkConfig;
import com.douyu.omdap.common.DyFlinkSupport;
import com.douyu.omdap.common.EscafKafkaMessage;
import com.douyu.omdap.common.FlinkTableConfigParse;
import com.douyu.omdap.common.annon.FlinkSQL;
import com.douyu.omdap.common.annon.FlinkTable;
import com.douyu.omdap.common.config.LocalFlinkConfig;
import com.douyu.omdap.common.function.LocalMorphlineFunction;
import com.douyu.omdap.util.ClassUtils;

/**
 * 暂时将这里设计为当前的形式，需要将具体的SQL配置逻辑进行分离设计 1.Annotation的方式 2.配置文件的方式或者其他方式
 * 这里暂时默认为Annotation配置
 * <p>
 * 监控数据任务,主要是负责对日志、监控数据的一些关键字、指标等进行流式查询，捕获异常数据 该功能主要的任务：数据解析、数据查询监控、
 */
public class MonitorDataJobUtil {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void startKafkaSourceMonitorDataJob(Class<?> clazz) throws Exception {

		TypeInformation<?> typeInformation = Types.POJO(clazz);

		AbstractFlinkConfig flinkConfig = new LocalFlinkConfig("flink-mysql.properties");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		env.setMaxParallelism(4);

		DataStream<EscafKafkaMessage> dataStream = env.addSource(
				DyFlinkSupport.createKafkaConsumer010JSON(true, new LocalFlinkConfig("flink-golbale-conf.properties")));

		DataStream analyseStream = dataStream.map(new LocalMorphlineFunction()).returns(typeInformation);

		FlinkTable flinkTable = AnnotationUtils.getAnnotation(clazz, FlinkTable.class);
		if (null == flinkTable) {
			throw new NullPointerException("you must config annon FlinkTable");
		}

		tEnv.registerDataStream(flinkTable.tableName(), analyseStream, flinkTable.columns());

		FlinkSQL[] flinkSQL = flinkTable.SQL();
		for (FlinkSQL sql : flinkSQL) {
			Table table = tEnv.sqlQuery(sql.sql());
			DataStream resuDataStream = null;
			if (ArrayUtils.isEmpty(sql.resultAttrs()) && ArrayUtils.isEmpty(sql.aggResultAttrs())) {

				resuDataStream = tEnv.toAppendStream(table, typeInformation);
			} else if (!ArrayUtils.isEmpty(sql.resultAttrs())) {
				TypeInformation resultTypeInformation = ClassUtils.getQuerySQLResultType(clazz, sql.resultAttrs());
				resuDataStream = tEnv.toAppendStream(table, resultTypeInformation);
			}

			if (!ArrayUtils.isEmpty(sql.aggResultAttrs())) {
				TypeInformation resultTypeInformation = ClassUtils
						.getQuerySQLResultTypeRowTypeInfo(sql.aggResultAttrs());
				resuDataStream = tEnv.toRetractStream(table, resultTypeInformation);
			}

		}

		env.execute();
	}

	public static void startSocketSourceAnalyseJob(Class<?> clazz) throws Exception {

		TypeInformation<?> typeInformation = Types.POJO(clazz);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		env.setMaxParallelism(4);

		DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 8085);

		DataStream<EscafKafkaMessage> escafKafkaMessageStream = dataStream
				.map(new MapFunction<String, EscafKafkaMessage>() {

					private static final long serialVersionUID = 1L;

					@Override
					public EscafKafkaMessage map(String value) throws Exception {
						try {
							EscafKafkaMessage msg = new EscafKafkaMessage();
							msg.setValue(value);
							msg.setTopic("nginx");
							return msg;
						} catch (Exception e) {
							System.out.println(e);
						}
						return null;
					}
				});

		DataStream analyseStream = escafKafkaMessageStream.map(new LocalMorphlineFunction()).returns(typeInformation);

		FlinkTable flinkTable = AnnotationUtils.getAnnotation(clazz, FlinkTable.class);
		if (null == flinkTable) {
			throw new NullPointerException("you must config annon FlinkTable");
		}

		tEnv.registerDataStream(flinkTable.tableName(), analyseStream, flinkTable.columns());

		FlinkSQL[] flinkSQL = flinkTable.SQL();
		for (FlinkSQL sql : flinkSQL) {
			Table table = tEnv.sqlQuery(sql.sql());
			DataStream resuDataStream = null;

			if (ArrayUtils.isEmpty(sql.resultAttrs()) && ArrayUtils.isEmpty(sql.aggResultAttrs())) {

				resuDataStream = tEnv.toAppendStream(table, typeInformation);
			} else if (!ArrayUtils.isEmpty(sql.resultAttrs())) {
				TypeInformation resultTypeInformation = ClassUtils.getQuerySQLResultType(clazz, sql.resultAttrs());
				resuDataStream = tEnv.toAppendStream(table, resultTypeInformation);
			}

			if (!ArrayUtils.isEmpty(sql.aggResultAttrs())) {
				TypeInformation resultTypeInformation = ClassUtils
						.getQuerySQLResultTypeRowTypeInfo(sql.aggResultAttrs());
				resuDataStream = tEnv.toRetractStream(table, resultTypeInformation);
			}

			resuDataStream.print();
		}

		env.execute();
	}

	public static void startKafkaSourceMonitorDataJob0(String jobName, String monitorConfigFileName,
			String flinkConfigFile) throws Exception {

		FlinkTableConfigParse flinkTableConfigParse = new FlinkTableConfigParse(monitorConfigFileName);
		AbstractFlinkConfig flinkConfig = new LocalFlinkConfig(flinkConfigFile);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		JobParameters parameters = new JobParameters();
		parameters.put("morphline", "h3c-switch0");
		env.getConfig().setGlobalJobParameters(parameters);

		// env.registerCachedFile("file:///devops/linghan/flink-1.6.2/job-config/morphline/h3c-switch",
		// "h3c-switch");

		env.registerCachedFile("file:///E:/intellij/dy-omdap/src/main/resources/morphline-regex/h3c-switch0",
				"h3c-switch0");

		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		env.enableCheckpointing(60000L);
		env.setMaxParallelism(4);

		DataStream<EscafKafkaMessage> dataStream = env
				.addSource(DyFlinkSupport.createKafkaConsumer010JSON(true, flinkConfig));

		DataStream analyseStream = dataStream.map(new LocalMorphlineFunction())
				.returns(flinkTableConfigParse.getAnalyseTypeInformation());

		tEnv.registerDataStream(flinkTableConfigParse.getTableName(), analyseStream,
				flinkTableConfigParse.getTableColumnName());
		String querySQLS[] = flinkTableConfigParse.getQuerySQL();
		for (String sql : querySQLS) {

			Table table = tEnv.sqlQuery(sql);
			DataStream resultDataStream = tEnv.toAppendStream(table, Row.class);
			resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));

		}

		String aggSQLS[] = flinkTableConfigParse.getAggSQL();
		if (null != aggSQLS) {
			for (String aggSql : aggSQLS) {
				Table table = tEnv.sqlQuery(aggSql);
				RelNode tableRelNode = table.getRelNode();
				boolean isAppendOnly = UpdatingPlanChecker.isAppendOnly(tEnv.optimize(tableRelNode, false));
				DataStream resultDataStream = null;
				if (isAppendOnly) {
					resultDataStream = tEnv.toAppendStream(table,
							flinkTableConfigParse.getQueryAggTypeInformation(aggSql));
					resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				} else {
					resultDataStream = tEnv.toRetractStream(table, Row.class);
					resultDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Row>() {

						@Override
						public Row map(Tuple2 value) throws Exception {

							return Row.of(value.f1);
						}

					}).addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				}
			}

		}

		env.execute(jobName);

	}
	@SuppressWarnings({"rawtypes","unchecked"})
	public static void startKafkaSourceMonitorDataJob(String jobName, String monitorConfigFileName,
			String flinkConfigFile) throws Exception {

		FlinkTableConfigParse flinkTableConfigParse = new FlinkTableConfigParse(monitorConfigFileName);
		AbstractFlinkConfig flinkConfig = new LocalFlinkConfig(flinkConfigFile);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		JobParameters parameters = new JobParameters();
		parameters.put("morphline", "h3c-switch");
		env.getConfig().setGlobalJobParameters(parameters);

		// env.registerCachedFile("file:///devops/linghan/flink-1.6.2/job-config/morphline/h3c-switch",
		// "h3c-switch");

		env.registerCachedFile("file:///E:/intellij/dy-omdap/src/main/resources/morphline-regex/h3c-switch",
				"h3c-switch");

		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		env.enableCheckpointing(60000L);
		env.setMaxParallelism(4);

		DataStream<EscafKafkaMessage> dataStream = env
				.addSource(DyFlinkSupport.createKafkaConsumer010JSON(true, flinkConfig));

		
		DataStream analyseStream = dataStream.map(new LocalMorphlineFunction())
				.returns(flinkTableConfigParse.getAnalyseTypeInformation());

		tEnv.registerDataStream(flinkTableConfigParse.getTableName(), analyseStream,
				flinkTableConfigParse.getTableColumnName());
		String querySQLS[] = flinkTableConfigParse.getQuerySQL();
		for (String sql : querySQLS) {

			Table table = tEnv.sqlQuery(sql);
			DataStream resultDataStream = tEnv.toAppendStream(table, Row.class);
			resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));

		}

		String aggSQLS[] = flinkTableConfigParse.getAggSQL();
		if (null != aggSQLS) {
			for (String aggSql : aggSQLS) {
				Table table = tEnv.sqlQuery(aggSql);
				RelNode tableRelNode = table.getRelNode();
				boolean isAppendOnly = UpdatingPlanChecker.isAppendOnly(tEnv.optimize(tableRelNode, false));
				DataStream resultDataStream = null;
				if (isAppendOnly) {
					resultDataStream = tEnv.toAppendStream(table,
							flinkTableConfigParse.getQueryAggTypeInformation(aggSql));
					resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				} else {
					resultDataStream = tEnv.toRetractStream(table, Row.class);
					resultDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Row>() {

						
						@Override
						public Row map(Tuple2 value) throws Exception {

							return Row.of(value.f1);
						}

					}).addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				}
			}

		}

		env.execute(jobName);

	}

	/**
	 * 约定：配置文件的名称是和任务名称是一致的！ 传入指定的配置文件即可启动任务
	 *
	 * @param monitorConfigFileName
	 *            任务名称
	 * @throws Exception
	 */
	public static void startSocketSourceMonitorDataJob(String jobName, String monitorConfigFileName,
			String flinkConfigFile) throws Exception {

		FlinkTableConfigParse flinkTableConfigParse = new FlinkTableConfigParse(monitorConfigFileName);
		AbstractFlinkConfig flinkConfig = new LocalFlinkConfig(flinkConfigFile);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		env.setMaxParallelism(4);

		DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 8085);

		DataStream<EscafKafkaMessage> escafKafkaMessageStream = dataStream
				.map(new MapFunction<String, EscafKafkaMessage>() {

					private static final long serialVersionUID = 1L;

					@Override
					public EscafKafkaMessage map(String value) throws Exception {

						EscafKafkaMessage msg = new EscafKafkaMessage();
						msg.setValue(value);
						msg.setTopic("h3c-switch");// h3c-switch
						return msg;
					}
				});

		// 注意这里别将proceTime属性加入到类型中去,但是下面的注册表的时候要使用全部的字段包含proctime,类型绑定也要全部包含
		DataStream analyseStream = escafKafkaMessageStream.map(new LocalMorphlineFunction())
				.returns(flinkTableConfigParse.getAnalyseTypeInformation());

		tEnv.registerDataStream(flinkTableConfigParse.getTableName(), analyseStream,
				flinkTableConfigParse.getTableColumnName());
		String querySQLS[] = flinkTableConfigParse.getQuerySQL();

		for (String sql : querySQLS) {

			Table table = tEnv.sqlQuery(sql);
			DataStream resultDataStream = tEnv.toAppendStream(table, flinkTableConfigParse.getDefaultTypeInformation());
			resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));

		}

		String aggSQLS[] = flinkTableConfigParse.getAggSQL();
		if (null != aggSQLS) {
			for (String aggSql : aggSQLS) {
				Table table = tEnv.sqlQuery(aggSql);
				RelNode tableRelNode = table.getRelNode();
				boolean isAppendOnly = UpdatingPlanChecker.isAppendOnly(tEnv.optimize(tableRelNode, false));
				DataStream resultDataStream = null;
				if (isAppendOnly) {
					resultDataStream = tEnv.toAppendStream(table,
							flinkTableConfigParse.getQueryAggTypeInformation(aggSql));
					resultDataStream.addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				} else {
					resultDataStream = tEnv.toRetractStream(table, Row.class);
					resultDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Row>() {

						@Override
						public Row map(Tuple2 value) throws Exception {

							return Row.of(value.f1);
						}

					}).addSink(DyFlinkSupport.createHttpSinkFunction(flinkConfig));
				}
			}

		}

		env.execute(jobName);
	}

	private static class JobParameters extends ExecutionConfig.GlobalJobParameters {
		private static final long serialVersionUID = -8118611781035212808L;
		private Map<String, String> parameters;

		private JobParameters() {
			this.parameters = new HashMap<String, String>();

		}

		public void put(String key, String value) {
			parameters.put(key, value);
		}

		@Override
		public Map<String, String> toMap() {
			return parameters;
		}
	}

}
