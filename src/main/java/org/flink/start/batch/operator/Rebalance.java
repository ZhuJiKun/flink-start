package org.flink.start.batch.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

public class Rebalance {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(4);
        DataSet<String> dataSet = env.fromElements("orange", "red", "yellow", "blue", "black", "java");


        /*
         *  强制重新平衡数据集，即，数据集均匀分布于以下任务的所有并行实例中。这可以帮助在严重数据倾斜和计算密集型操作的情况下提高性能。
         *  重要:该操作将整个数据集在网络上进行了洗牌，并且花费大量的时间。
         */

        // 以统计字符串中出现次数最多的字符次数为例
        DataSet<Tuple2<String, Integer>> result = dataSet.rebalance()
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        char[] chs = s.toCharArray();
                        Arrays.sort(chs);

                        int max = 0, temp = 0;
                        char t = chs[0];

                        for (char c : chs) {
                          if (c == t) {
                              temp ++;
                              continue;
                          }

                          max = Math.max(max, temp);
                          temp = 1;
                        }

                        return Tuple2.of(s, Math.max(max, temp));
                    }
                });
        result.print();
    }

}
