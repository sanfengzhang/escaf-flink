package com.escaf.flink.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractFlinkConfig {

	protected Map<String, String> cacheConfig = new ConcurrentHashMap<String, String>();

	public abstract String get(String key);

	public abstract String get(String key, String defaultValue);

	public abstract String getRequried(String key);

	public int getInteger(String key) {

		return Integer.parseInt(get(key));

	}

	public int getInteger(String key, int defaultValue) {
		String value = get(key);
		if (null == value || "".equals(value.trim())) {
			return defaultValue;
		}

		return Integer.parseInt(value);

	}

	public double getDouble(String key) {

		return Double.parseDouble(get(key));

	}

	public boolean getBoolean(String key) {
		return Boolean.valueOf(get(key));
	}

	public boolean getBoolean(String key, boolean defaultValue) {
		String value = get(key);
		if (null == value || "".equals(value.trim())) {
			return defaultValue;
		}

		return Boolean.valueOf(get(key));

	}

	public Map<String, String> getCacheConfig() {
		return cacheConfig;
	}

}
