package com.escaf.flink.common.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.Test;

import com.escaf.flink.common.AbstractFlinkConfig;
import com.escaf.flink.common.LocalFlinkConfig;

public class DriverClassTest {

	private AbstractFlinkConfig flinkConfig = new LocalFlinkConfig();

	@Test
	public void selectSql() throws SQLException {

		final Connection connection = DriverManager.getConnection(
				"jdbc:mysql://192.168.1.40:3306/han?useUnicode=true&characterEncoding=UTF-8", "root", "1234");

		String sql = "select * from tb_test_date";

		PreparedStatement statement = connection.prepareStatement(sql);

		ResultSet rs = statement.executeQuery();

		while (rs.next()) {
			System.out.println(rs.getTimestamp(3));
			System.out.println(rs.getString(4));

		}

		statement.close();
		connection.close();
	}

	@Test
	public void insertTypeTest() throws SQLException {
		final Connection connection = DriverManager.getConnection(
				"jdbc:mysql://192.168.1.40:3306/han?useUnicode=true&characterEncoding=UTF-8", "root", "1234");

		String sql = "insert into tb_test_date (start_time,start_timestamp,start_datetime) values (?,?,?)";

		PreparedStatement statement = connection.prepareStatement(sql);

		statement.setTime(1, Time.valueOf("20:10:55"));

		statement.setTimestamp(2, Timestamp.valueOf("2018-05-22 11:08:22.777888999"));

		statement.setTimestamp(3, Timestamp.valueOf("2018-05-22 11:08:22.777888999"));

		statement.execute();
		statement.close();

		connection.close();

	}

	@Test
	public void connectionTest() throws SQLException, IOException {
		final Connection connection = DriverManager.getConnection(flinkConfig.getRequried("flink.sink.dburl"),
				flinkConfig.getRequried("flink.jdbc.username"), flinkConfig.getRequried("flink.jdbc.passwd"));

		for (int i = 0; i < 5; i++) {
			final String sql = "insert into t_class values(" + i + ")";
			new Thread() {
				@SuppressWarnings("static-access")
				public void run() {
					PreparedStatement statement;
					try {

						statement = connection.prepareStatement(sql);
						System.out.println(statement);
						statement.execute(sql);
						Thread.currentThread().sleep(5000L);
						System.out.println(System.currentTimeMillis());
					} catch (SQLException e) {

						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}.start();
		}

		System.in.read();
	}

}
