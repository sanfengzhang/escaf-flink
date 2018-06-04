package com.escaf.flink.app.transinfotask;

import java.util.Map;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class TransSimpleCondition extends IterativeCondition<Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	private int rowNum;

	public TransSimpleCondition(int rowNum) {
		super();
		this.rowNum = rowNum;
	}

	@Override
	public boolean filter(Map<String, Object> value, Context<Map<String, Object>> ctx) throws Exception {

		try {

			int rowNum0 = Integer.valueOf(value.get("rowNum").toString());
			if (rowNum0 != rowNum) {
				return false;
			}
		} catch (Exception e) {
			System.out.println(e);
			return false;
		}

		return true;
	}
}