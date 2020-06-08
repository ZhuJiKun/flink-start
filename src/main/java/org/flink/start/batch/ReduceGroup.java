package org.flink.start.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReduceGroup {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 5, 4, 6, 8, 0, 10, 2);

        /**
         * 将一组元素组合成一个或多个元素。ReduceGroup可以应用于完整的数据集，也可以应用于分组后的数据集。
         *
         * 如果将reduce应用于分组的数据集，则可以通过提供CombinHint作为第二个参数来指定运行时执行reduce的combining阶段的方式。
         * 在大多数情况下，基于散列的策略应该更快，尤其是如果不同键的数量少于输入元素的数量（例如1/10）时。
         */
        DataSet<Long> newDateSet = dataSet.reduceGroup(new GroupReduceFunction<Integer, Long>() {

            /**
             * @param iterable 元素迭代器
             * @param collector 元素采集器
             */
            @Override
            public void reduce(Iterable<Integer> iterable, Collector<Long> collector) throws Exception {
                long num = 0;
                for (Integer integer : iterable) {
                    num += integer;
                    collector.collect(num);
                }
            }
        });

        // 每次都输出累加的值
        newDateSet.print();
    }

}
