package org.flink.start.batch.operator.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class HashPartition {

    private final static Object VAL = new Object();

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(8);

        DataSet<Tuple2<String, Object>> dataSet = env.fromElements(
                Tuple2.of("orange", VAL),
                Tuple2.of("red", VAL),
                Tuple2.of("yellow", VAL),
                Tuple2.of("blue", VAL),
                Tuple2.of("black", VAL),
                Tuple2.of("java", VAL),
                Tuple2.of("php", VAL),
                Tuple2.of("c++", VAL));

        // 根据指定key的哈希值对数据集进行分区，某一key集中时还是会出现数据倾斜
        dataSet = dataSet.partitionByHash(0);

        DataSet<Tuple2<String, String>> result = dataSet.map(new MapFunction<Tuple2<String, Object>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, Object> s) throws Exception {
                return Tuple2.of(s.f0, Thread.currentThread().getName());
            }
        });

        result.print();
    }
}
