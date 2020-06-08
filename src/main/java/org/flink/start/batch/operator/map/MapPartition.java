package org.flink.start.batch.operator.map;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MapPartition {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3, 4, 5);

        /**
         * 优点：
         *  如果是普通的map，比如一个partition中有1万条数据,那么你的function要执行和计算1万次。
         *  使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有的partition数据。只要执行一次就可以了，性能比较高。
         *
         *  缺点：
         *   如果是普通的map操作，一次function的执行就处理一条数据；
         *   但是MapPartitions操作，对于大量数据来说，比如甚至一个partition/task，100万数据，一次传入一个function以后，
         *   那么可能一下子内存不够，但是又没有办法去腾出内存空间来，可能就OOM，内存溢出。
         */

        dataSet = dataSet.mapPartition(new MapPartitionFunction<Integer, Integer>() {

            /**
             * @param iterable 此partition/task下的元素的迭代器
             * @param collector 元素收集器
             */
            @Override
            public void mapPartition(Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                for (Integer integer : iterable) {
                    collector.collect(integer * 2); // 输出 元素*2
                }
            }
        });

        dataSet.print();
    }
}
