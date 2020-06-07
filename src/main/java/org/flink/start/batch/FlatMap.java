package org.flink.start.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMap {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3, 4, 5);

        /**
         * 取一个元素并产生零个，一个或多个元素。
         * 与map不同，map会直接返回一个元素， flatMap用collector收集器来收集多个元素
         */
        dataSet = dataSet.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer num, Collector<Integer> collector) throws Exception {
                // 输出 元素 与 元素的相反数
                collector.collect(num);
                collector.collect(-num);
            }
        });

        dataSet.print();
    }
}
