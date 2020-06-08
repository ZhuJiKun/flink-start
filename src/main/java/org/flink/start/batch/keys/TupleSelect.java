package org.flink.start.batch.keys;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Tuple key选择器
 */
public class TupleSelect {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
         * 使用Tuples来指定key
         */
        DataSet<Tuple2<Integer, Integer>> dataSet = env.fromElements(
                Tuple2.of(1, 10),
                Tuple2.of(1, 20),
                Tuple2.of(2, 30),
                Tuple2.of(2, 40));

        // 使用下标, 从0开始
        dataSet.groupBy(0).sum(1).print();

        // 使用字段名称 f0 f1 f2 f3...
        dataSet.groupBy("f0").sum(1).print();

        /*
         * 如果Tuple是嵌套的格式，
         *   例如：DataStream<Tuple3<Tuple2<Integer, Float>,String,Long>> ds，
         *   如果指定keyBy(0)则会使用内部的整个Tuple2作为key。
         *   如果想要使用内部Tuple2中的Float格式当做key，可以使用keyBy("f0.f1")这样的形式指定,可以使用"xx.f0"表示嵌套tuple中第一个元素，也可以直接使用”xx.0”来表示第一个元素。
         *
         * 如果需要指定多个字段当做联合的Key，可以写成keyBy(0,1)
         * 如果写成字符串形式在字符串中指定多个key，还可以写成keyBy("f0","f1")的形式。
         */
    }

}
