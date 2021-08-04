package com.dlwlrma.flink.api.processfuntion;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author hex1n
 * @Date 2021/8/4 13:11
 * @Description
 */
public class ProcessTest1_keyedProcessFunction {
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
        // 测试KeyedProcessFunction ,先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print("ssssssss");


        env.execute();
    }

    // 实现自定义的处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {
        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsTimer", Long.class));
            super.open(parameters);
        }

        @Override
        public void processElement(SensorReading value, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(value.getId().length());

            // context
            context.timestamp();
            context.getCurrentKey();
//            context.output(); // 侧输出流操作
            long currentTime = context.timerService().currentProcessingTime();
            context.timerService().currentWatermark();
            context.timerService().registerProcessingTimeTimer(currentTime + 10000L);
            // 保存当前定时触发时间
            tsTimerState.update(currentTime + 10000L);
            context.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000); // 当前时间加10秒
//            context.timerService().deleteEventTimeTimer(10000L);
            // 清理当前定时器
//            context.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "  定时器触发");
            Tuple currentKey = ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain(); // 时间域
            super.onTimer(timestamp, ctx, out);
        }
    }
}
