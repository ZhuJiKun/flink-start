package org.flink.start.batch.operator.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 根据指定的key对数据集进行范围分区
 */
public class RangePartition {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(4);
        DataSet<String> dataSet = env.fromElements("orange", "red", "yellow", "blue", "black", "java");


        /*
         * 在Flink批处理的优化器中，会专门针对RangePartition算子进行一次优化，
         * 主要是通过采样算法对数据进行估计，并修改原job生成的OptimizedPlan。
         * 可参考 https://blog.csdn.net/u013036495/article/details/85560640
         */
        dataSet.partitionByRange(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        })

                // 输出
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s + ":" + Thread.currentThread().getName();
                    }
                }).print();

    }

}
