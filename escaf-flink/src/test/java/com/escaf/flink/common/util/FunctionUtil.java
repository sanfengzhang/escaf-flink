package com.escaf.flink.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class FunctionUtil {

	@Test
	public void intiMorpline() throws FileNotFoundException, IOException {

		Jedis jedis = new Jedis("192.168.1.40", 6379);

		Map<String, String> map = new HashMap<String, String>();

		String value = IOUtils.toString(new FileInputStream(new File("C:\\Users\\owner\\Desktop\\temp.conf")));

		map.put("trans_log_test", value);
		jedis.hmset("Morphline", map);

		jedis.close();

	}

	@Test
	public void getTranslog() {

		Jedis jedis = new Jedis("192.168.1.40", 6379);

		Map<String, String> map = jedis.hgetAll("Morphline");

		System.out.println(map.get("trans_log_test"));

		jedis.close();

	}

}
