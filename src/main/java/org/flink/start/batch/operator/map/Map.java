package org.flink.start.batch.operator.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Map {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3, 4, 5);

        /**
         * map: 取一个元素并产生一个元素。
         */
        dataSet = dataSet.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer num) throws Exception {
                return num * 2;  // 输出 元素*2
            }
        });

        dataSet.print();
    }

}
