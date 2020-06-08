package org.flink.start.batch.operator.join;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Union {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
         * 根据第一个字段做分组，拼接相同值的第二个字段
         */
        DataSet<Tuple2<Long, String>> dataSet1 = env.fromElements(
                Tuple2.of(1L, "A"),
                Tuple2.of(2L, "B"));

        DataSet<Tuple2<Long, String>> dataSet2 = env.fromElements(
                Tuple2.of(2L, "C"),
                Tuple2.of(1L, "D"),
                Tuple2.of(3L, "E"));

        // 产生两个数据集的并集,类型需一致
        DataSet<Tuple2<Long, String>> result = dataSet1.union(dataSet2);
        result.print();
    }
}
