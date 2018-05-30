package com.escaf.flink.common.function;

import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.escaf.flink.common.AbstractFlinkConfig;

public class JdbcRowFunction extends RichFlatMapFunction<Record, Row> {

	private static final long serialVersionUID = 1L;

	private String[] columnArrays = null;

	private String columns = null;

	public JdbcRowFunction(AbstractFlinkConfig flinkConfig) {

		columns = flinkConfig.get("flink.jdbcoutputformat.cloumnname");

	}

	@Override
	public void open(Configuration parameters) throws Exception {

		if (StringUtils.isEmpty(columns)) {
			throw new NullPointerException();
		}

		columnArrays = columns.split(",");

		if (columnArrays == null || columnArrays.length == 0) {
			throw new NullPointerException();
		}

	}

	@Override
	public void flatMap(Record value, Collector<Row> out) throws Exception {
		int len = columnArrays.length;
		Object[] result = new Object[len];
		for (int i = 0; i < len; i++) {
			result[i] = value.get(columnArrays[i]);
		}

		out.collect(Row.of(result));

	}
}
