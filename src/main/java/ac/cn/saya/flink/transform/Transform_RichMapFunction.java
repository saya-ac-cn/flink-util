package ac.cn.saya.flink.transform;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基本算子转换（map,flatMap,filter）
 * @Title: Transform_Base
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 10:13
 * @Description:
 */

public class Transform_RichMapFunction {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
        DataStream<Tuple2<String, Double>> resultStream = dataStream.map(new DiyRichMapFunction());
        // 打印
        resultStream.print();
        // 提交执行
        env.execute();
    }

}

class DiyRichMapFunction extends RichMapFunction<SensorReading, Tuple2<String,Double>>{
    @Override
    public Tuple2<String, Double> map(SensorReading val) throws Exception {
        return new Tuple2<>(val.getId(), val.getTemperature());
    }

    /**
     * 只要任务一旦创建就会执行
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("打开数据库连接");
    }

    @Override
    public void close() throws Exception {
        System.out.println("关闭数据库连接");
    }
}