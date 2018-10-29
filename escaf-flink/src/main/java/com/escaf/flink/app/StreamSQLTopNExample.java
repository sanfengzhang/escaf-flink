package com.escaf.flink.app;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class StreamSQLTopNExample {

	public static void main(String[] args) {

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Order> input = env.fromElements(new Order(1, "beer", 3), new Order(1, "diaper", 4),
				new Order(3, "rubber", 2));
		
		
		


	}

}

class Order {

	private int id;

	private String proId;

	private int amount;

	public Order(int id, String proId, int amount) {
		this.id = id;
		this.proId = proId;
		this.amount = amount;
	}

}
