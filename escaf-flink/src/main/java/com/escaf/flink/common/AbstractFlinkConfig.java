package com.escaf.flink.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractFlinkConfig {

	protected Map<String, String> cacheConfig = new ConcurrentHashMap<String, String>();

	protected AbstractFlinkConfig() {
	}

	protected AbstractFlinkConfig(String resourceFileName) {

		InputStream inStream = null;
		try {
			ClassLoader classLoader = this.getClass().getClassLoader();
			inStream = classLoader.getResourceAsStream(resourceFileName);

			Properties p = new Properties();
			p.load(inStream);

			Iterator<Entry<Object, Object>> it = p.entrySet().iterator();

			while (it.hasNext()) {
				Entry<Object, Object> en = it.next();

				cacheConfig.put(en.getKey().toString(), en.getValue().toString());

			}

		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			if (null != inStream) {
				try {
					inStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

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
