package com.escaf.flink.common;

import java.lang.reflect.InvocationTargetException;

import javax.activation.UnsupportedDataTypeException;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class BaseEscafJob {

	private static final String FLINK_CONFIG_CLASS = "com.escaf.flink.common.LocalFlinkConfig";

	protected AbstractFlinkConfig flinkConfig = null;

	/**
	 * 缺省的支持本地文件配置
	 */
	public BaseEscafJob() {

		createInstance(FLINK_CONFIG_CLASS, null);

	}

	public BaseEscafJob(String resourceFileName) {

		if (resourceFileName.contains("redis")) {
			createInstance(RedisFlinkConfig.class.getName(), resourceFileName);
		} else if (resourceFileName.contains("local")) {
			createInstance(FLINK_CONFIG_CLASS, resourceFileName);
		} else {
			try {
				throw new UnsupportedDataTypeException("do not support this config,filename=" + resourceFileName);
			} catch (UnsupportedDataTypeException e) {
				e.printStackTrace();
			}
		}

	}

	private void createInstance(String clazzName, String resourceFileName) {
		try {
			@SuppressWarnings("unchecked")
			Class<AbstractFlinkConfig> cfgInstance = (Class<AbstractFlinkConfig>) Class.forName(clazzName);

			if (null == resourceFileName) {
				flinkConfig = cfgInstance.newInstance();
			} else {
				flinkConfig = cfgInstance.getConstructor(String.class).newInstance(resourceFileName);
			}

			if (null == flinkConfig) {

				throw new NullPointerException("init AbstractFlinkConfig Object must not null ");

			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {

			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public abstract void start() throws Exception;

	public ParameterTool getGlobalJobParameters() {
		ParameterTool parameterTool = ParameterTool.fromMap(flinkConfig.getCacheConfig());
		return parameterTool;
	}

}
