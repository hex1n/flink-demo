package com.dlwlrma.flink.api.processfuntion;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author hex1n
 * @Date 2021/8/6 13:27
 * @Description 案例: 高低温分流
 */
public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("E:\\Self\\flink-demo\\src\\main\\resources\\sensorReading.txt");
        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("10.11.2.153", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 定义一个OutputTag 用来表示侧输出流 低温流--- 后面加花括号表示使用匿名内部类的实现 避免泛型擦除
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };
        // 测试KeyedProcessFunction ,自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context context, Collector<SensorReading> collector) throws Exception {
                // 判断温度, 大于30度 表示高温流输出到主流/ 小于30 度表示低温流,输出到侧输出流
                if (value.getTemperature() > 30) {
                    collector.collect(value);
                } else {
                    context.output(lowTempTag,value);
                }
            }
        });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag);


        env.execute();
    }
}
