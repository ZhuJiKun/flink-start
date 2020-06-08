package org.flink.start.batch.operator.join;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class Cross {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, String>> dataSet1 = env.fromElements(
                Tuple2.of(1L, "A"),
                Tuple2.of(2L, "B"));

        DataSet<Tuple2<Long, String>> dataSet2 = env.fromElements(
                Tuple2.of(2L, "C"),
                Tuple2.of(1L, "D"),
                Tuple2.of(3L, "E"));

        /*
         * Cross笛卡尔积
         * Cross可能是一项非常消耗计算量的操作，甚至可能会挑战大型计算集群！建议通过使用crossWithTiny()和crossWithHuge()来提示系统具有DataSet大小。
         */
        boolean useCrossFunction = false;

        if (!useCrossFunction) {
            DataSet<Tuple2<Tuple2<Long, String>, Tuple2<Long, String>>> result = dataSet1.cross(dataSet2);
            result.print();
            return;
        }

        /*
         * 可使用CrossFunction将一对元素转换为单个元素
         */
        DataSet<Tuple4<Long, String, Long, String>> result = dataSet1.cross(dataSet2)
                .with(new CrossFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple4<Long, String, Long, String>>() {
                    @Override
                    public Tuple4<Long, String, Long, String> cross(Tuple2<Long, String> t1, Tuple2<Long, String> t2) throws Exception {
                        return Tuple4.of(t1.f0, t1.f1, t2.f0, t2.f1);
                    }
                });
        result.print();
    }

}
