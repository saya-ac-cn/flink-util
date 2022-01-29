package ac.cn.saya.flink.transform;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: Transform_RollingAggregation
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 12:35
 * @Description:
 */

public class Transform_Reduce {

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
        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        DataStream<SensorReading> resultStream = keyedStream.reduce((lastVal,currentVal)-> new SensorReading(lastVal.getId(),currentVal.getTimestamp(),Math.max(lastVal.getTemperature(), currentVal.getTemperature())));
        // 打印
        resultStream.print();
        // 提交执行
        env.execute();
    }
}
