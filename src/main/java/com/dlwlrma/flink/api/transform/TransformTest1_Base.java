package com.dlwlrma.flink.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hex1n
 * @date 2021/6/22 14:37
 * @description
 */
public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\learn\\flink\\flink-demo\\src\\main\\resources\\words.txt");
        // 1 map 把String 转换成长度输出
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) String::length);
        // 2 flatmap 按空格分字段
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });
        // 3 filter 筛选包含o 的对应数据
        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>) value -> value.contains("o"));

        // 打印输出
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");
        env.execute();
    }

}
