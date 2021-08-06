package com.dlwlrma.flink.api.demo;

import org.apache.commons.compress.utils.Sets;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
        DataStreamSource<String> dataStreamSource = env.socketTextStream("10.11.2.153", 7777);
        DataStream<BehaviorInfo> dataStream = dataStreamSource.map(value -> {
            String[] fields = value.split(",");
            return new BehaviorInfo(fields[0], fields[1], fields[2]);
        });
        dataStream.keyBy(new KeySelector<BehaviorInfo, Object>() {
                    @Override
                    public Tuple2<String, String> getKey(BehaviorInfo behaviorInfo) throws Exception {
                        return Tuple2.of(behaviorInfo.getAgentId(), behaviorInfo.getUserId());
                    }
                })
                .timeWindow(Time.seconds(20))
                .process(new ProcessWindowFunction<BehaviorInfo, ScoreInfo, Object, TimeWindow>() {
                    ValueState<Long> scoreState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        scoreState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("scoreState", Long.class,0L));
                    }

                    @Override
                    public void process(Object key, Context context, Iterable<BehaviorInfo> iterable, Collector<ScoreInfo> collector) throws Exception {
                        System.out.println(key + "key=======");
                        Set<String> behaviors = Sets.newHashSet();
                        iterable.forEach(behaviorInfo -> behaviors.add(behaviorInfo.getBehavior()));
                        for (BehaviorInfo behaviorInfo : iterable) {
                            if (singleStrategyList.contains(behaviorInfo.getBehavior())) {
                                System.out.println("ooooooooooooooooooo");
                                Long value = scoreState.value();
                                System.out.println(value + "VVVVVVVVVVVVVVVVVVVV11");
                                scoreState.update(value + 1);
                                System.out.println(scoreState.value() + "RRRRRRRRRRRRRRRRR11");
                            }
                            if (associationStrategyList.containsAll(behaviors)) {
                                System.out.println("===============");
                                Long value = scoreState.value();
                                System.out.println(value + "VVVVVVVVVVVVVVVVVVVV222");
                                scoreState.update(value + 2);
                                System.out.println(scoreState.value() + "RRRRRRRRRRRRRRRRR22");
                            }
                        }
                    }
                }).addSink(new RichSinkFunction<ScoreInfo>() {
                    @Override
                    public void invoke(ScoreInfo value, Context context) throws Exception {

                        super.invoke(value, context);
                    }
                });
        env.execute();
    }


}
