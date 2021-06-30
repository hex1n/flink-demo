package com.dlwlrma.flink.api.sink;

import com.dlwlrma.flink.pojo.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.List;

/**
 * @author hex1n
 * @date 2021/6/29 16:40
 * @description
 */
public class SinkTest3_Es {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\learn\\flink\\flink-demo\\src\\main\\resources\\sensorReading.txt");
        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 定义es连接配置
        List<HttpHost> httpHosts = Lists.newArrayList();
        httpHosts.add(new HttpHost("localhost", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());
        env.execute();
    }

    // 实现自定义的es 写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer requestIndexer) {
            // 定义写入的数据source
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("temp", element.getTemperature().toString());
            dataSource.put("ts", element.getTimestamp().toString());

            // 创建请求作为向es 发起的写入命令
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);
            // 用index发送请求
            requestIndexer.add(indexRequest);
        }
    }
}
