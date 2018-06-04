package com.escaf.flink.app.transinfotask;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.flink.cep.PatternSelectFunction;

public class TradePatternSelectFunction implements PatternSelectFunction<Map<String, Object>, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String select(Map<String, List<Map<String, Object>>> pattern) throws Exception {
		Set<Entry<String, List<Map<String, Object>>>> set = pattern.entrySet();
		Iterator<Entry<String, List<Map<String, Object>>>> it = set.iterator();

		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			Entry<String, List<Map<String, Object>>> en = it.next();
			String name = en.getKey();
			List<Map<String, Object>> records = en.getValue();
			if (records.size() > 1) {
				System.out.println("something wrong");
			}

			sb.append(name).append(",").append(records.get(0).toString());

		}
		return sb.toString();
	}

}
