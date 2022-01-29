package ac.cn.saya.flink.source;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中读取
 * @Title: SourceTest1
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 08:55
 * @Description:
 */

public class SourceTest_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从集合中读取数据
        DataStream<SensorReading> sourceStream = env.fromCollection(Arrays.asList(new SensorReading("Sensor_1", 1547718201L, 15.4),
                new SensorReading("Sensor_2", 1547718201L, 21.5),
                new SensorReading("Sensor_3", 1547718201L, 38.4),
                new SensorReading("Sensor_4", 1547718201L, 6.7)));

        // 打印输出
        sourceStream.print();

        // 执行
        env.execute();
    }

}
