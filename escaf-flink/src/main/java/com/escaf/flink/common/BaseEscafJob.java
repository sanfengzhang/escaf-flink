package com.escaf.flink.common;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class BaseEscafJob {

	private static final String FLINK_CONFIG_CLASS = "com.escaf.flink.common.LocalFlinkConfig";

	protected AbstractFlinkConfig flinkConfig = null;

	public BaseEscafJob() {

		try {
			@SuppressWarnings("unchecked")
			Class<AbstractFlinkConfig> cfgInstance = (Class<AbstractFlinkConfig>) Class.forName(FLINK_CONFIG_CLASS);
			flinkConfig = cfgInstance.newInstance();
			if (null == flinkConfig) {

				throw new NullPointerException("init AbstractFlinkConfig Object must not null ");

			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {

			e.printStackTrace();
		} catch (IllegalAccessException e) {

			e.printStackTrace();
		}

	}

	public abstract void start() throws Exception;

	public ParameterTool getGlobalJobParameters() {
		ParameterTool parameterTool = ParameterTool.fromMap(flinkConfig.getCacheConfig());
		return parameterTool;
	}

}
