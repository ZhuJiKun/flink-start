package org.flink.start;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * see https://www.cnblogs.com/importbigdata/p/10779957.html
 *
 * 如果报NoClassDefFoundError错误
 * see https://blog.csdn.net/weixin_38842096/article/details/85723335
 * 需要倒入flink的jar包
 *
 * 打包之后可以直接提交到flink系统上
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {

		// 设置执行环境
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 输入的数据
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
		);

		/**
		 * see https://blog.csdn.net/chybin500/article/details/87260869
		 * flink中的算子是将一个或多个DataStream转换为新的DataStream，可以将多个转换组合成复杂的数据流拓扑。
		 * 在Flink中，有多种不同的DataStream类型，他们之间是使用各种算子进行的。
		 * 有以下算子：
		 *  map： 可以理解为映射，对每个元素进行一定的变换后，映射为另一个元素。  1 --> 1
		 *  flatmap: 将元素摊平，每个元素可以变为0个、1个、或者多个元素。
		 *  filter: 进行筛选
		 *  keyBy: Stream根据指定的Key进行分区，是根据key的散列值进行分区的。
		 */


		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
						.groupBy(0) //以第0个下表进行分组
						.sum(1); // 统计第1个下表

		// execute and print result
		counts.print();
		//counts.writeAsText("/Users/zhujikun/quickstart/src/main/resources/result.csv", FileSystem.WriteMode.OVERWRITE);
	}


	/**
	 * map函数
	 *
	 * Tuple2 类似map的概念，一个元组
	 * out.collect（）
	 * 返回的DateSet里面是(to,1) (be, 1) (ro, 1)...  等等
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
