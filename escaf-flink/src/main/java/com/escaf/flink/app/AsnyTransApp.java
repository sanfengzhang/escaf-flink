package com.escaf.flink.app;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Flink 的异步 I/O API允许用户在数据流中使用异步请求客户端。API处理与数据流的集成，以及处理顺序，事件时间，容错等。

	假设有一个用于目标数据库的异步客户端，要实现一个通过异步I/O来操作数据库还需要三个步骤：
	实现调度请求的 AsyncFunction
	获取操作结果并把它传递给 ResultFuture 的 callBack
	将异步 I/O 操作作为转换操作应用于 DataStream
 * 
 *上述大概是描述异步IO的处理的一些特征、异步其实最主要的就是解决在数据流中需要依赖其他的一些外部的信息对数据进行处理：
 *例如查询静态库、http请求等这些操作在原始的流处理模型中、当数据A发送查询请求的时候、后面的B数据是被阻塞住的了。
 *这样会导致系统吞吐下降和延迟升高、为了缓解这类问题、我们可以采用异步IO请求的方式去处理数据。
 *Flink AsynIO 是怎么做到的呢？
 *
 *它是对流处理的数据进行了一层封装、将对象封装成promise对象、简单来说就是先返回一个对象的引用给你、当你需要获取执行结果的
 *时候去调用一些方法就好了。属于一种异步编程的方式,不阻塞当前线程的运行、
 *
 */

public class AsnyTransApp {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(2);

		DataStream<Integer> sourceStream = env.socketTextStream("192.168.1.101", 8085).map(new Tokenizer());

		DataStream<User> resultStream = AsyncDataStream.orderedWait(sourceStream, new QueryUserInfoFunction(), 20,
				TimeUnit.SECONDS);

		resultStream.print();

		env.execute("asyn execute job");

	}

}

final class Tokenizer implements MapFunction<String, Integer> {
	private static final long serialVersionUID = 1L;

	@Override
	public Integer map(String value) throws Exception {

		return Integer.valueOf(value);
	}

}

class User implements Serializable {
	private static final long serialVersionUID = 1L;

	int id;

	String userName;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", userName=" + userName + "]";
	}

}

class QueryUserInfoFunction extends RichAsyncFunction<Integer, User> {

	private static final long serialVersionUID = 1L;

	private transient DruidDataSource dataSource;

	private ListeningExecutorService service = null;

	@Override
	public void open(Configuration parameters) throws Exception {

		super.open(parameters);
		dataSource = new DruidDataSource();
		dataSource.setMaxActive(10);
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/han ");
		dataSource.setUsername("root");
		dataSource.setPassword("1234");

		service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
	}

	@Override
	public void asyncInvoke(Integer input, ResultFuture<User> resultFuture) throws Exception {		
		ListenableFuture<ResultSet> future = service.submit(new Callable<ResultSet>() {
			public ResultSet call() throws Exception {
				ResultSet resultSet = dataSource.getConnection()
						.prepareStatement("select id,user_name from t_user where id=" + input).executeQuery();
				return resultSet;
			}
		});

		//理解这段代码的调用关系、
		//当future从Mysql中号查询到数据的时候会接着调用触发onSuccess的方法、这个时候onSuccess会将处理获取的User对象
		//注册到resultFuture中，那么就可以获取到异步查询的结果。CompletableFuture的complete方法是将future的状态设置为完成
		//这里只是完成了对future的对象引用的生成、和在future执行完成之后进行一些方法的调用。那么在Flink是在哪里触发future.get的
		//呢？这样才会真正执行future获得结果？
		//在Flink 异步模型中所有的元素都会封装为StreamRecordBufferEntry对象、
		Futures.addCallback(future, new FutureCallback<ResultSet>() {
			public void onSuccess(ResultSet resultSet) {

				resultFuture.complete(proccessUsers(resultSet));

			}

			public void onFailure(Throwable thrown) {

				resultFuture.completeExceptionally(thrown);

			}
		});

	}

	private Collection<User> proccessUsers(ResultSet rs) {
		try {
			if (rs.next()) {
				int id = rs.getInt(1);
				String userName = rs.getString(2);

				User user = new User();

				user.setId(id);
				user.setUserName(userName);

				System.out.println(user.toString());
				return Collections.singleton(user);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;

	}

	@Override
	public void close() throws Exception {

		super.close();

		if (null != service) {
			service.shutdown();
		}

		if (null != dataSource) {
			dataSource.close();
		}
	}

}
