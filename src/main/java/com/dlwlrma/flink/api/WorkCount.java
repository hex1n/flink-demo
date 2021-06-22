package com.dlwlrma.flink.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author hex1n
 * @date 2021/4/20 12:45
 * @description
 */
public class WorkCount {

    public static void main(String[] args) throws Exception {

        // 批处理
        // 创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "D:\\learn\\flink\\flink-demo\\src\\main\\resources\\words.txt";
        // 从文件中读取数据
        DataSet<String> inputDataSet = env.readTextFile(filePath);
        // 对数据集进行处理,按空格分词展开,转换成(word ,1) 二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        //注意.这里的Tuple2包要是org.apache.flink.api.java.tuple.Tuple2
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);
        resultSet.print();
    }

    //自定义类,实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = s.split(" ");
            // 遍历所有word,包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
