package ac.cn.saya.flink.sink;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @Title: Sink_Kafka
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 22:56
 * @Description:
 */

public class Sink_Kafka{

    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");
        // 将文本数据清洗成对象数据
        DataStream<String> dataStream = sourceStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2])).toString();
            }
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>("127.0.0.1:9092","sinktest",new SimpleStringSchema()));
    }

}
