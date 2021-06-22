package com.dlwlrma.flink.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/** 以kafka 消息队列的数据作为来源
 * @author hex1n
 * @date 2021/6/22 11:55
 * @description
 */
public class SourceTest2_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "39.105.192.123:9092");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("TOPIC-HX-TEST", new SimpleStringSchema(), properties));
        // 打印输出
        dataStream.print();
        env.execute();
    }
}
