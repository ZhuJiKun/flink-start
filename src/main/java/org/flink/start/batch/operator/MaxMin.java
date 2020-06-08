package org.flink.start.batch.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MaxMin {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, Integer, Long>> dataSet = env.fromElements(
                Tuple3.of("a", 1, 3L),
                Tuple3.of("a", 1, 5L),
                Tuple3.of("b", 2, 4L),
                Tuple3.of("c", 3, 9L)
        );

        /*
         * 从一个或多个字段的值最小（最大）的一组元组中选择一个元组。
         * 用于比较的字段必须是有效的关键字段，即可比较的字段。
         * 如果多个元组具有最小（最大）字段值，则返回这些元组中的任意元组。
         * MinBy（MaxBy）可以应用于完整的数据集或分组的数据集。
         */
        dataSet.max(1).print();

        // 比较2个
        dataSet.minBy(1, 2).print();

        // 先分组，再比较
        dataSet.groupBy(1).maxBy(2).print();

    }
}
