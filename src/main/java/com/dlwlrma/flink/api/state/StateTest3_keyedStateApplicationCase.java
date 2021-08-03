package com.dlwlrma.flink.api.state;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @Author hex1n
 * @Date 2021/8/3 18:04
 * @Description
 */
public class StateTest3_keyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\learn\\flink\\flink-demo\\src\\main\\resources\\sensorReading.txt");
        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        /**
         * 利用 Keyed State  实现这样一个需求: 检测传感器的温度值,如果连续两个温度差值超过10度,就输出报警
         */
        // 定义一个flatMap 操作,检查温度跳变,输出报警
        SingleOutputStreamOperator<Tuple3<String,Double,Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        env.execute();

    }

    // 实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 私有属性,温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态 ,保存上一次温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempState",Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();
            // 如果状态不为null 就判断两次温度差值
            if (lastTemp!=null){
                Double diff =Math.abs(sensorReading.getTemperature()-lastTemp);
                if (diff>=threshold){
                    collector.collect(new Tuple3<>(sensorReading.getId(),lastTemp,sensorReading.getTemperature()));
                }
            }
            // 更新状态
            lastTempState .update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
