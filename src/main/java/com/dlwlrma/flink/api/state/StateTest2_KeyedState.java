package com.dlwlrma.flink.api.state;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author hex1n
 * @Date 2021/8/3 13:44
 * @Description 键控状态 只能在RichFunction 中使用
 */
public class StateTest2_KeyedState {

    public static void main(String[] args) {
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

        // 定义一个有状态的map 操作,统计当前sensor 数据个数
        dataStream.keyBy("id").map(new MyKeyCountMapper());
    }

    // 自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> keyCountState;
        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String,Double> myMapState;
        private ReducingState<SensorReading> myReducingState;


        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class,0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map",String.class,Double.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reducing",,SensorReading.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);

            // 其他状态API调用
            // listState
            Iterable<String> strings = myListState.get();
            myListState.add("hello");
            for (String string : strings) {
                System.out.println(string);
            }
            // mapState
            Double aDouble = myMapState.get("1");
            myMapState.put("2",2.1);
            myMapState.remove("2");
            // reducing
//            myReducingState.add(sensorReading);
            // 清空所有状态
            myListState.clear();
            return count;

        }
    }
}
