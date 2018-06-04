package com.escaf.flink.app.transinfotask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class TradeInfoSplitSelector implements OutputSelector<Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	private final String[] transIps;

	public TradeInfoSplitSelector(String[] transIps) {
		super();
		this.transIps = transIps;
	}

	@Override
	public Iterable<String> select(Map<String, Object> value) {
		List<String> output = new ArrayList<String>();
		String ip = value.get("ip").toString();
		for (String ipStr : transIps) {
			if (ip.equals(ipStr)) {
				output.add(ipStr);

			}

		}

		return output;
	}
}
