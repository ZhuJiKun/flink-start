package org.flink.start.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 更多的join连接可参考 https://www.cnblogs.com/asker009/p/11074417.html
 */
public class OuterJoin {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet1 = env.fromElements("a", "b", "b", "c", "c", "c");
        DataSet<String> dataSet2 = env.fromElements("x");

        DataSet<Tuple2<String, Integer>> input1 = dataSet1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        input1 = input1.groupBy(0).sum(1);

        DataSet<Tuple2<String, Integer>> input2 = dataSet2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        input2 = input2.groupBy(0).sum(1);


        /*
         * 以左连接为例
         * 右连接和全连接实现一致
         */
        DataSet<Tuple3<String, String, Integer>> result = input1.leftOuterJoin(input2)
                .where(1)
                .equalTo(1)

                // 可以自定义JoinFuncation
                .with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> join(Tuple2<String, Integer> t, Tuple2<String, Integer> tt) throws Exception {
                        if (tt == null) {
                            return new Tuple3<>(t.f0, null,  t.f1);
                        }
                        return new Tuple3<>(t.f0, tt.f0,  t.f1);
                    }
                });

        result.print();
    }

}
