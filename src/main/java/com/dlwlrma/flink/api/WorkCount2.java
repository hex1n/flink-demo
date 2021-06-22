package com.dlwlrma.flink.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hex1n
 * @date 2021/4/20 12:45
 * @description
 */
public class WorkCount2 {

    public static void main(String[] args) throws Exception {
        // 流处理
        // 创建流处理运行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        String filePath = "D:\\learn\\flink\\flink-demo\\src\\main\\resources\\words.txt";
        // 从文件中读取数据
//        DataStream<String> inputDataStream = env.readTextFile(filePath);
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WorkCount.MyFlatMapper())
                .keyBy(item -> item.f0)
                .sum(1);
        resultStream.print();
        //执行任务 这里可以理解为前面的代码是在定义任务,只有执行env.execute()后 Flink
        // 才把前面的代码片段当作一个任务整体(每个线程根据这个任务操作,并行处理流数据);
        env.execute();
    }
}
