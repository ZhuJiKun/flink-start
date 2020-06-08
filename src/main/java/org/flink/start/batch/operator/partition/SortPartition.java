package org.flink.start.batch.operator.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 以指定顺序对指定字段上的数据集的所有分区进行本地排序。
 *
 * TODO: 待验证
 */
public class SortPartition {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);

        DataSet<Integer> dataSet = env.fromElements(1,2,3,4,5,6,7,8);

        dataSet.sortPartition(new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }, Order.DESCENDING)
                .map(new MapFunction<Integer, String>() {
                    @Override
                    public String map(Integer integer) throws Exception {
                        return integer + " : " + Thread.currentThread().getName();
                    }
                }).print();
    }

}
