package ac.cn.saya.flink.sink;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Title: Sink_ElasticSearch
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 23:17
 * @Description:
 */

public class Sink_ElasticSearch{

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");
        // 将文本数据清洗成对象数据
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });

        // es 的 配置信息
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1",9200));
        // 通过sink写入到redis
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new DiyElasticsearchSinkFunction()).build());

        // 提交执行
        env.execute();
    }

}

class DiyElasticsearchSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
    @Override
    public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        Map<String, String> dataSource = new HashMap<>();
        dataSource.put("id",sensorReading.getId());
        dataSource.put("timestamp",sensorReading.getTimestamp().toString());
        dataSource.put("temperature",sensorReading.getTemperature().toString());
        IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingData").source(dataSource);
        requestIndexer.add(indexRequest);
    }
}