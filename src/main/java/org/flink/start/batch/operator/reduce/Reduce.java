package org.flink.start.batch.operator.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Reduce {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 5, 4, 6, 8, 0, 10, 2);

        /**
         * 两个元素组合为一个元素， 将一组元素组合为一个元素
         * Reduce可以应用于完整的数据集，也可以应用于分组的数据集。
         */
        dataSet = dataSet.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer integer, Integer t1) throws Exception {
                if (integer == null) {
                    return t1 == null ? Integer.MIN_VALUE : t1;
                }
                if (t1 == null) {
                    return integer;
                }
                return integer > t1 ? integer : t1;
            }
        });

        // 输出最大值
        dataSet.print();
    }
}
