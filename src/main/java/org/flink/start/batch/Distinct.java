package org.flink.start.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Distinct {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(1, 2, 5, 4, 6, 8, 0, 10, 2);

        // 去重, 可以对相对于元素的所有字段或字段的子集去重, 如果对某个字段去重需要用Tuple
        dataSet.distinct().print();
    }
}
