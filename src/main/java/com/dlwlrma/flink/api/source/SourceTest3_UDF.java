package com.dlwlrma.flink.api.source;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 自定义实现source
 *
 * @author hex1n
 * @date 2021/6/22 14:13
 * @description
 */

public class SourceTest3_UDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> dataStream = env.addSource(new MyCustomerSource());
        // 打印输出
        dataStream.print();
        env.execute();
    }

    // 实现自定义的SourceFunction

    public static class MyCustomerSource implements SourceFunction<SensorReading> {

        // 定义一个标志位 用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置10 各传感器的初始温度
            HashMap<String, Double> sensorTempMap = Maps.newHashMap();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 再当前温度基础上做随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                // 控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
