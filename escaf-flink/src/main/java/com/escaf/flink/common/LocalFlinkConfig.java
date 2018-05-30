package com.escaf.flink.common;

import java.io.IOException;

public class LocalFlinkConfig extends AbstractFlinkConfig {

	private static final String def_resource_name = "local.flink.properties";

	public LocalFlinkConfig() {

		super(def_resource_name);

	}

	public LocalFlinkConfig(String resourceFileName) throws IOException {

		super(resourceFileName);

	}

	@Override
	public String get(String key) {

		return cacheConfig.get(key);
	}

	@Override
	public String get(String key, String defaultValue) {
		String value = get(key);
		if (null == value || value.trim().equals("")) {
			return defaultValue;
		}

		return value;
	}

	@Override
	public String getRequried(String key) {
		String value = get(key);
		if (null != value) {
			return value;
		}
		throw new NullPointerException("requried key");
	}

}
