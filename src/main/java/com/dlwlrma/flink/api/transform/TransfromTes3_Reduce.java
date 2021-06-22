package com.dlwlrma.flink.api.transform;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hex1n
 * @date 2021/6/22 15:07
 * @description
 */
public class TransfromTes3_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\learn\\flink\\flink-demo\\src\\main\\resources\\sensorReading.txt");
        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);


        // reduce聚合,取当前最大的温度值,以及当前的最新的时间戳
        DataStream<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(),value2.getTimestamp(),Math.max(value1.getTemperature(),value2.getTemperature()));
            }
        });
        resultStream.print();
        env.execute();
    }
}
