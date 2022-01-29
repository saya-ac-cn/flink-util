package ac.cn.saya.flink.source;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 自定义数据来源
 * @Title: SourceTest_Diy
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 09:54
 * @Description:
 */

public class SourceTest_Diy {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从集合中读取数据
        DataStream<SensorReading> sourceStream = env.addSource(new DiySensorSource());

        // 打印输出
        sourceStream.print();

        // 执行
        env.execute();
    }

}
