package com.escaf.flink.app;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 这个是对在窗口处理中指定提取eventTime的方法和水印时间的获取。
 * extractTimestamp方法中的作用是将当前事件的时间作为currentTimestamp变量的值，
 * currentTimestamp主要用于生成水印的作用，在这里是水印的时间比当前事件早5S,结合窗口计算的意思可以理解为
 * 首先Flink会将窗口计算划分为：[0-5000)、[5000-10000)、[10000-15000)...等等划分窗口。
 * 
 * 那么Flink是怎么触发窗口的计算的呢？
 * 先理解水印时间的意义，水印时间watermark(T)发送就是说：所有eventTime(T)<watermark(T)的事件已经发送完毕了。
 * 从该程序中可知计算[5000-10000)这个窗口触发应该是：（W(T)=
 * currentTimestamp-5000）>15000,所以currentTimestamp>20000的时候才出发
 * w(t)>15000的水印这个时候是可以触发计算（还有一些其他判断，暂时忽略分析）
 * 
 * 在这里面有个问题join的时候我们知道Flink会在每个流里面都有watermark,但是窗口时一致的、那么Flink何时触发计算呢？
 * 因为我两个流的watermark可能速度不一样了？
 * 去查阅源代码我们知道：Flink对多个channel的watermark会做对齐处理、并且从中取一个最小的watermark进行更新和后续的处理
 * {@StatusWatermarkValve#inputWatermark(Watermark, int)}
 * 
 * 
 * 在这一块代码功能分析的方面： {@HeapInternalTimerService#advanceWatermark}这个里面是触发Window计算的调用
 */

public class JoinStream {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final long windowSize = params.getLong("windowSize", 5000);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		// env.getConfig().setGlobalJobParameters(params);

		DataStream<Tuple3<String, Long, Long>> stream1 = getSource(env, 8085)
				.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks());

		DataStream<Tuple3<String, Long, Long>> stream2 = getSource(env, 8086)
				.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks());

		// TumblingEventTimeWindows按照事件时间来进行窗口计算
		DataStream<Tuple4<String, Long, Long, Long>> joinedStream = stream1.join(stream2).where(new MyJoinKeyselector())
				.equalTo(new MyJoinKeyselector()).window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
				.apply(new JoinFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple4<String, Long, Long, Long> join(Tuple3<String, Long, Long> first,
							Tuple3<String, Long, Long> second) throws Exception {

						System.out.println("execute window");

						return new Tuple4<String, Long, Long, Long>(first.f0, first.f1, second.f1, first.f2);
					}

				});

		joinedStream.print().setParallelism(1);

		// System.out.println(env.getExecutionPlan());

		// execute program
		env.execute("Windowed Join Example");

	}

	private static DataStream<Tuple3<String, Long, Long>> getSource(StreamExecutionEnvironment env, int port) {

		DataStream<Tuple3<String, Long, Long>> sourceStream = env.socketTextStream("192.168.1.101", port)
				.map(new Tokenizer());

		return sourceStream;

	}

	private static final class MyAssignerWithPeriodicWatermarks
			implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Long>> {

		private static final long serialVersionUID = 1L;

		private long currentTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(Tuple3<String, Long, Long> element, long previousElementTimestamp) {
			final long newTimestamp = element.f2;
			if (newTimestamp >= this.currentTimestamp) {
				this.currentTimestamp = newTimestamp;
				return newTimestamp;
			}
			return newTimestamp;
		}

		@Override
		public Watermark getCurrentWatermark() {

			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 5000);
		}

	}

	private static final class MyJoinKeyselector implements KeySelector<Tuple3<String, Long, Long>, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple3<String, Long, Long> value) throws Exception {

			return value.f0;
		}
	}

	private static final class Tokenizer implements MapFunction<String, Tuple3<String, Long, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, Long, Long> map(String value) throws Exception {
			String[] tokens = value.toLowerCase().split("\\W+");

			Tuple3<String, Long, Long> result = new Tuple3<String, Long, Long>();
			result.f0 = tokens[0].trim();
			result.f1 = Long.parseLong(tokens[1]);
			result.f2 = Long.parseLong(tokens[2]);
			return result;

		}
	}

}
