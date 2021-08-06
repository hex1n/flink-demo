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
 * @Date 2021/8/6 12:57
 * @Description 案例:检测一段时间内温度上连续上升,输出报警
 */
public class ProcessTest2_ApplicationCase {

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
                .process(new TempConsIncreWarning(10))
                .print("ssssssss");


        env.execute();
    }

    // 实现自定义处理函数,检测一段时间内温度上连续上升,输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        // 定义私有属性,当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态,保存上一个温度值,定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-Temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-Ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            // 如果温度上升并且没有定时器的时候,注册10秒后的定时器,开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                // 计算定时器时间戳
                Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果温度下降,那么删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发,输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值联系" + interval + "s 连续上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            super.close();
        }
    }
}
