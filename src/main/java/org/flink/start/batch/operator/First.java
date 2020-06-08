package org.flink.start.batch.operator;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 前N个
 */
public class First {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> dataSet = env.fromElements(
                Tuple2.of(10, 1),
                Tuple2.of(1, 21),
                Tuple2.of(1, 1),
                Tuple2.of(12, 0)
        );

        /*
         * 返回数据集的前n个（任意）元素。
         * First-n可以应用于常规数据集，分组数据集或分组排序数据集。
         * 可以将分组键指定为键选择器功能或字段位置键。
         */

        dataSet.first(2).print();

        // 每一组都输出前1个
        dataSet.groupBy("f0").first(1).print();

        // 每一组，组内排序，排序完输出第一个
        dataSet.groupBy("f0").sortGroup(1, Order.ASCENDING).first(1).print();
    }

}
