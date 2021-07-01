package com.dlwlrma.flink.api.window;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hex1n
 * @date 2021/6/30 19:04
 * @description
 */
public class WindowTest1_TimeWindow {

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

        // 窗口测试  ----增量聚合测试
        DataStream<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(10, 20);
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
//                .window(EventTimeSessionWindows.withGap(Time.seconds(15)));
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        // 创建一个累加器
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        // 主要在Seession window 中用到 合并操作
                        return integer + acc1;
                    }
                });

        // 全窗口函数
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                /*.process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {

                    }
                })*/
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    /**
                     *
                     * @param tuple 当前的键
                     * @param timeWindow
                     * @param input
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long windEnd = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        collector.collect(new Tuple3<>(id, windEnd, count));
                    }
                });
        // 其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .trigger(new Trigger<SensorReading, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(SensorReading sensorReading, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

                    }
                })
                .evictor(new Evictor<SensorReading, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<SensorReading>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<SensorReading>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }
                })
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<SensorReading>("late") {
                })
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");

        resultStream.print("result1=========>");
        resultStream2.print("result2=========>");
        env.execute();
    }
}
