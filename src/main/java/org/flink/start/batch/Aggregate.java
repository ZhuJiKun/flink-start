package org.flink.start.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Aggregate {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 5, 4, 6, 8, 0, 10, 2);

        // Aggregating on field positions is only possible on tuple data types.
        DataSet<Tuple2<Integer, Integer>> set = dataSet.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        });

        /**
         * 聚合函数, 多个值聚为一个值
         * sum max min maxBy minBy
         *
         * min只返回计算的最小值，而最小值对应的其他数据不保证正确。
         * minBy返回计算的最小值，并且最小值对应的其他数据是保证正确的。
         * max maxBy同理
         */
        set.groupBy(0).sum(1).maxBy(1).print();
    }

}
