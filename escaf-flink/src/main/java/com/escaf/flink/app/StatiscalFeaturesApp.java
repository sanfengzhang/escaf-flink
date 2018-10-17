package com.escaf.flink.app;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import com.escaf.flink.common.BaseEscafJob;
import com.escaf.flink.common.EscafFlinkSupport;

/**
 * # 构建对应KPIID的计算数据，包含对原始值、时间序列、时间序列特征组合的最终算法所使用的数据 """ 在该KPI异常检测算法中的思路：
 * 先对数据进行清洗、然后将数据按照 time value label的方式组装成一个多维向量、并且是按时间进行排序的 是否可以组合成按
 * label----value、时序特征这样对应成的一个函数Y=AX+BT的分类函数、自己的思路想试着用 SVR向量回归去做异常检测
 * 有个问题，那么待检测数据如何进行检测了？ 因为上述生成的回归方程是关于value、时序特征的函数，而待 检测的只有一个时间点、和value、怎么调用？？？
 * 一样的算法求出该待测数据的时序特征、这样代入方程得出结果、但是这样准确吗？
 * 
 * 下面是对回答该问题的一些理论支持，从数学的角度来科学的描述这种解决方案的合理性： 关于特征工程的构建： 可以从实时和离线两个方面去构建：
 * 1.实时构建的特征工程主要是基于时间序列去构建相关的特征、存储到特征库 将时间窗口的一些特征计算出来、结合Flink.
 * 2.将每条数据按照时间窗口抽象成一条数据、包含各种特征、原始值、均值、最大值、最小值、标准差、
 * 加权移动平均值、使用多种数学特征、趋势特征、变化特征、对窗口进行描述、然后我们可以对窗口进行聚类
 * 异常窗口和正常窗口进行聚类分析。（需要找到一些相关性较强的特征） 3.通过序列前后值的对比计算对比特征 4.通过不同宽度的窗口去计算、窗口宽度特征
 * 5.在对训练数据进行分析的时候、其实在异常发生的一段时间内、其第一次出现的异常是相当重要的。
 * 
 * 关于时间序列数据特征： 窗口特征 转化特征 差分统计特征 分解特征
 * 
 * 6.在选择窗口类型的时候怎么选择呢？有时间翻滚窗口、时间滑动窗口、和按次数计算的窗口。
 * 在这里先选择按时间滑动窗口、那么在这一窗口选型中、这个是一条数据可以在多个窗口中同时存在的， 那么最终异常检测是怎么实施呢？
 * 
 * 7.在使用滑动平均、加权滑动平均算法时候、是可以对下一个时刻的值进行预测值、但是我们在实时计算中同时能获得真实值 那么如何建立预测值和真实值之间的关系呢？
 * 
 * 8.在测试数据的时候如何选择窗口大小是比较合理的这个也很重要
 * 
 * 
 * 
 *
 * 将窗口聚合数据、和窗口特征计算分开实现：
 * 现阶段Flink对python的开发支持不是很完善，但是需要借助Flink的流计算和窗口机制，在Flink触发窗口计算的时候只是将数据进行存储或者
 * 通过接口发送到python计算系统、分析窗口的特征
 *
 * 
 * 
 * 后续模型的持续改进： 3.如何结合实时数据、历史数据进行不断的对KPI异常判断标准进行实时的调整模型参数。
 * 
 * 说一下自己很早之前就在想KPI异常检测究竟该怎么做，但是不得其法。因为局限于对数据的认识总是从单条数据去理解，
 * 单条数据很难建立模型关系，也很难找到一些其他特征。但是如果将数据按照时间序列划分为一个个的小的时间序列数据段，那
 * 么就能从数据微观变化趋势去分析。这样对KPI的异常分析也更具有说服性。再一次印证在解决一些比较难的问题的时候、我们
 * 需要不断的去阅读一些现有的资料、能从那些资料中挖掘出更深入的一些东西从而解决问题。也说明往往在阅读一些资料的时候
 * 只是流于表面的一些意思，而没有深入的去研究问题、
 * 
 * 
 * """
 * 
 * 
 * 
 * 
 *
 */
public class StatiscalFeaturesApp {

}

class StatiscalFeaturesJob extends BaseEscafJob {

	@Override
	public void start() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();
		env.getConfig()
				.setRestartStrategy(RestartStrategies.fixedDelayRestart(
						flinkConfig.getInteger(EscafFlinkSupport.RESATRT_ATTEMPTS, 1),
						flinkConfig.getInteger(EscafFlinkSupport.DELAY_BETWEEN_ATTEMPTS, 10000)));
		env.enableCheckpointing(flinkConfig.getInteger(EscafFlinkSupport.CHECKPOINT_INTERVAL, 5000));

		env.getConfig().setGlobalJobParameters(getGlobalJobParameters());
		@SuppressWarnings("unchecked")
		DataStream<String> kpiDataStream = env
				.addSource(EscafFlinkSupport.createKafkaConsumer010JSON(true, flinkConfig));

	}

}

class KPIWindowFunction implements WindowFunction<String, String, String, Window> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(String key, Window window, Iterable<String> input, Collector<String> out) throws Exception {

	}

}
