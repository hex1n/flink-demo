package com.dlwlrma.flink.api.demo;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author hex1n
 * @Date 2021/8/3 21:13
 * @Description
 */
public class Test02 {

    private static List<List<String>> associationStrategyList = Arrays.asList(
            Lists.newArrayList("readMorningPaper", "viewBusinessCard", "readArticle")
            , Lists.newArrayList("likedMorningPaper", "likedBusinessCard")
    );
    private static List<String> singleStrategyList = Arrays.asList(
            "readMorningPaper", "viewBusinessCard", "readArticle"
            , "likedMorningPaper", "likedBusinessCard"
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从mysql 中查询复合/单个策略
        /*DataStreamSource<List<List<String>>> listDataStreamSource = env.addSource(new JdbcReader());
        listDataStreamSource.broadcast().map(new MapFunction<List<List<String>>, Object>() {
            @Override
            public Object map(List<List<String>> lists) throws Exception {
                associationStrategyList = lists;
                return null;
            }
        });*/
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);
        DataStream<BehaviorInfo> dataStream = dataStreamSource.map(value -> {
            String[] fields = value.split(",");
            return new BehaviorInfo(fields[0], fields[1], fields[2], new Long(fields[3]));
        });
        dataStream.keyBy(new KeySelector<BehaviorInfo, Object>() {
                    @Override
                    public Tuple2<String, String> getKey(BehaviorInfo behaviorInfo) throws Exception {
                        return Tuple2.of(behaviorInfo.getAgentId(), behaviorInfo.getUserId());
                    }
                })
                .timeWindow(Time.minutes(5));

        env.execute();
    }


}
