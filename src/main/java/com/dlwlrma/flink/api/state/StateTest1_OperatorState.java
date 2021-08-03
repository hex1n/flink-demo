package com.dlwlrma.flink.api.state;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Author hex1n
 * @Date 2021/8/3 13:10
 * @Description 算子状态
 */
public class StateTest1_OperatorState {

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

        // 定义一个有状态的map 操作 统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());
        resultStream.print();

        env.execute();
    }

    // 自定义 MapFunction   存盘故障恢复机制 checkpoint
    public static class MyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {
        // 定义一个本地变量 作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // 统计个数
            return count++;
        }

        @Override
        public List<Integer> snapshotState(long checkpoint, long timestamp) throws Exception {
                // 快照存储 状态
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            // 取出checkpoint 中的状态  分区间的状态是各保存各的
            for (Integer num : state) {
                count+=num;
            }
        }
    }
}
