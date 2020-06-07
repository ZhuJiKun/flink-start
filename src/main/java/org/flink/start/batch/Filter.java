package org.flink.start.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Filter {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3, 4, 5);

        /**
         * 过滤，按照条件收集需要的元素
         * 注意：不可修改元素本身的内容，否则可能会造成不可预料的异常
         */
        dataSet = dataSet.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer num, Collector<Integer> collector) throws Exception {
                if (num % 2 == 0) {
                    collector.collect(num);
                }
            }
        });

        dataSet.print();
    }
}
