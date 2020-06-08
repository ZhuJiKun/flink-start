package org.flink.start.batch.operator.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.StringJoiner;

public class CoGroup {

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

        /*
         * 侧重于group，对同一个key上的两组集合进行操作。
         */
        DataSet<Tuple2<Long, String>> result = dataSet1.coGroup(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Long, String>> iterable, Iterable<Tuple2<Long, String>> iterable1,
                                        Collector<Tuple2<Long, String>> collector) throws Exception {

                        // 已经按照第一个字段分好组了，两个集合中元素的第一个字段值都相同
                        Long key = null;
                        StringJoiner sj = new StringJoiner(",", "[", "]");
                        for (Tuple2<Long, String> t : iterable) {
                            sj.add(t.f1);
                            key = t.f0;
                        }
                        for (Tuple2<Long, String> t : iterable1) {
                            sj.add(t.f1);
                            key = t.f0;
                        }
                        collector.collect(Tuple2.of(key, sj.toString()));
                    }
                });

        result.print();
    }
}
