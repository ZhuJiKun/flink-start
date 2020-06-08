package org.flink.start.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Join {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet1 = env.fromElements("a", "b", "b", "c", "c", "c");
        DataSet<String> dataSet2 = env.fromElements("x", "y", "y", "z", "z", "z");

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
         * 两个DataSet作join计算，统计出现次数一样的字符串以及次数
         * input1的第2个字段与input2的第2个字段相等做等值连接(内连接的一种)
         * 取input1的第1个字段,input2的第1个字段,input1的第2个字段作为输出
         */
        DataSet<Tuple3<String, String, Integer>> result = input1.join(input2)
                .where(1)
                .equalTo(1)
                .projectFirst(0)
                .projectSecond(0)
                .projectFirst(1);

        result.print();
        /*
         * 优化器会根据输入端的数据量大小与连接的类型，对于join操作实现有不同的实现策略
         * 也可以手动指定join策略
         * 参考 https://yq.aliyun.com/articles/259073
         */
    }
}
