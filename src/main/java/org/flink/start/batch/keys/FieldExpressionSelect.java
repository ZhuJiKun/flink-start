package org.flink.start.batch.keys;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 字段表达式
 */
public class FieldExpressionSelect {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
         * 类的访问级别必须是public
         * 必须写出默认的空的构造函数
         * 类中所有的字段必须是public的或者必须有getter，setter方法。
         * Flink必须支持字段的类型。
         */

        DataSet<Demo> dataSet = env.fromElements(
                new Demo(1, "tom"),
                new Demo(2, "mary"));

        dataSet.partitionByHash("name").print();
    }


    public static class Demo {
        public Demo() {
        }
        public Demo(Integer age, String name) {
            this.age = age;
            this.name = name;
        }

        private Integer age;
        private String name;

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Demo{" +
                    "age=" + age +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
