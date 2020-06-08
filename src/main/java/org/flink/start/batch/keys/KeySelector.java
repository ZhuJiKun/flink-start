package org.flink.start.batch.keys;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义key选择器
 */
public class KeySelector {

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Integer>> dataSet = env.fromElements(
                Tuple2.of("abc", 10),
                Tuple2.of("acd", 20),
                Tuple2.of("bdf", 30),
                Tuple2.of("cdf", 40));

        /*
         * 以字符串的第一个字符来去重
         */
        dataSet.distinct(new org.apache.flink.api.java.functions.KeySelector<Tuple2<String, Integer>, Character>() {
            @Override
            public Character getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0.charAt(0);
            }
        }).print();
    }
}
