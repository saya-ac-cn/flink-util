package ac.cn.saya.flink.transform;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @Title: Transform_RollingAggregation
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 12:35
 * @Description:
 */

public class Transform_MultipleStream {

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

        // 分流，按照温度值30摄氏度分为 低温 和 高温两条流
        SplitStream<SensorReading> splitStream = dataStream.split((OutputSelector<SensorReading>) val -> ((val.getTemperature()).compareTo(30D)>0)? Collections.singleton("high"):Collections.singleton("low"));
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");

        // 合流connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warringStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2 map(SensorReading val) throws Exception {
                return new Tuple2(val.getId(), val.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warringStream.connect(lowStream);
        DataStream<Tuple3<String, Double, String>> resultStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> val) throws Exception {
                // 来自高温的流数据
                return new Tuple3(val.f0, val.f1, "high");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReading val) throws Exception {
                // 来自低温的流数据
                return new Tuple3(val.getId(), val.getTemperature(), "low");
            }
        });

        // 打印
        //highStream.print("high");
        //lowStream.print("low");
        resultStream.print("connect");
        // 提交执行
        env.execute();
    }
}
