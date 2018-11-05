package com.escaf.flink.app;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 定义一个窗口需求：如5秒计算一次时间窗口、但是当窗口中的事件数>10的时候也可以触发窗口
 * 
 * 分析一下这个需求的可行性：实质上就是加了一个窗口执行的条件、任意满足其中一个就需要执行。思路：我们可以得到
 * 事件条数这个条件可能在5秒内满足或这不满足、那么我们是否可以基于TimeWindow做一个改造就是将事件计数条件也能触发窗口
 * 那么需要考虑的问题是：提前触发了window的计算、下一个窗口的startTime是怎么确定的是否有影响、对窗口状态清除是否有影响 这些都是需要考虑的问题
 * 
 * 问题在于：5秒内来了>10条数据、也就是提前触发window、那么在当前5S时间窗口后面的数据怎么分配窗口呢？假设我们将后续窗口分配到下面的
 * 窗口中去、那么我们时间窗口就是一个动态的窗口不是固定大小的时间窗口eg:[0-5),[5-7.3),[7.3,-12.3)....
 * 整理一下思路：那么我们首先要重写自定的窗口分配{@WindowAssigner}
 * 
 * 上述问题先放一下： 我们全面来学习一下window、系统的归纳window的一些性质、特点
 * 
 * 先看看window的分类： 1.按时间划分：时间滑动窗口（最近1分钟）、时间翻滚窗口(每隔1分钟计算一次)、 2.按计数进行划分:count window
 * 3.session window
 * 定义场景:在这种用户交互事件流中，我们首先想到的是将事件聚合到会话窗口中（一段用户持续活跃的周期），由非活跃的间隙分隔开。
 * 
 * 再来思考一下这些窗口在实际应用中能遇到的一些问题：
 * 1.实时不一定实时、延迟、乱序、Flink支持watermark的机制、在窗口计算中同样支持窗口的延迟计算 2.对于session
 * window我们的场景可能需要及时反馈用户会话状态信息通过计算并得出结果、假如很长一段时间都不产生session gap该怎么处理
 * 
 * 我们再看看在程序实现层面：在{@WindowOperator}这个类的处理方法中：是将window处理分为两个大类
 * 是MergingWindowAssigner实例和非MergingWindowAssigner实例两种：
 * 那么{@MergingWindowAssigner}是什么意思呢？它是集成{@WindowAssigner}的一个抽象类、额外的提供了window聚合的方法和一个callback接口
 * {@WindowAssigner}作用是对元素分配到一个或多个窗口中、指定缺省的trigger、窗口的序列化的一些操作
 * 
 * 再聊聊window的实现的一些基本点：
 * 从上面的window划分：Flink提供的GlobalWindow、TimeWindow(因为Flink在处理数据的时候可以结合自己的业务time维度进行分析：事件时间、处理事件、系统时间)
 * 
 * 
 * 
 * @author owner
 *
 */
public class AnalyseWindowStream {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(2);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Integer> sourceStream = env.socketTextStream("192.168.1.101", 8085).map(new Tokenizer());
		AllWindowedStream<Integer, TimeWindow> allWindowStream = sourceStream
				.timeWindowAll(Time.of(10, TimeUnit.SECONDS));

		// DataStream<Integer> reduceStream = testWindowReduceFunc(allWindowStream);
		DataStream<Integer> applyStream = testWindowApplyFunc(allWindowStream);

		// reduceStream.print();
		applyStream.print();

		env.execute("CustomizeTriggrtStream");

	}

	// 在{@AllWindowedStream}主要方法就是aggrate(这个是基于reduce去封装的)、reduce、fold（弃用）、apply，process所以总结起来就是两种
	// 这两大类function触发的机制是有区别的，详细见下面
	// 但是process和apply触发机制是一样的
	private static DataStream<Integer> testWindowApplyFunc(AllWindowedStream<Integer, TimeWindow> allWindowStream) {

		return allWindowStream.apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void apply(TimeWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
				// windowStream的apply方法是在时间窗口触发的时候才去执行自定义的AllWindowFunction函数，这个是针对窗口所有的数据
				// 这个函数的三个参数是：
				// IN ：input数据的类型、这个是当前窗口中所有的数据
				// OUT ：output对象的类型 输出的数据集
				// W : 继承自Window，表示需要在其上应用该操作的Window的类型
				// reduce函数和apply触发的机制是不一样的
				// 当前窗口触发是trigger的时候将emitWindowContents时候会触发

				System.out.println(System.currentTimeMillis());
				values.forEach(x -> System.out.println(x));

			}
		});
	}

	private static DataStream<Integer> testWindowReduceFunc(AllWindowedStream<Integer, TimeWindow> allWindowStream) {

		return allWindowStream.reduce(new ReduceFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {

				// reduce函数在窗口中的解释：value1是输入的值、value2是窗口中经过reduce函数计算的值存储、是一个累积的
				// eg:在当前例子中求平均值：在窗口初始的时候reduce计算的结果是null，当第一次来了value的时候不做reduce操作
				// 只有当window中对应的reduce值不为空的时候才调用reduce函数code:
				// {@HeapReducingState#ReduceTransformation}return previousState != null ?
				// reduceFunction.reduce(previousState, value) : value;
				// 在Flink中一些默认的reduce操作sum、max、min都是使用reduce函数去处理
				System.out.println("execute reduce...");
				return (value1 + value2) / 2;
			}
		});
	}
}
