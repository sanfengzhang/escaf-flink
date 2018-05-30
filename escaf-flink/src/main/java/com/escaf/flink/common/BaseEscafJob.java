package com.escaf.flink.common;

import javax.activation.UnsupportedDataTypeException;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class BaseEscafJob {

	private static final String FLINK_CONFIG_CLASS = "com.escaf.flink.common.LocalFlinkConfig";

	protected AbstractFlinkConfig flinkConfig = null;

	/**
	 * 缺省的支持本地文件配置
	 */
	public BaseEscafJob() {

		createInstance(FLINK_CONFIG_CLASS);

	}

	public BaseEscafJob(String resourceFileName) {

		if (resourceFileName.contains("redis")) {
			createInstance(RedisFlinkConfig.class.getName());
		} else if (resourceFileName.contains("local")) {
			createInstance(FLINK_CONFIG_CLASS);
		} else {
			try {
				throw new UnsupportedDataTypeException("do not support this config,filename=" + resourceFileName);
			} catch (UnsupportedDataTypeException e) {
				e.printStackTrace();
			}
		}

	}

	private void createInstance(String clazzName) {
		try {
			@SuppressWarnings("unchecked")
			Class<AbstractFlinkConfig> cfgInstance = (Class<AbstractFlinkConfig>) Class.forName(clazzName);
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
