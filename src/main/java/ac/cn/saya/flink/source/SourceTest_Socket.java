package ac.cn.saya.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket中读取
 * @Title: SourceTest_File
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 09:05
 * @Description:
 * 通过socket实时监听数据流 nc -lk 9000
 */

public class SourceTest_Socket {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket中读取
        DataStream<String> sourceStream = env.socketTextStream("127.0.0.1", 9000);
        // 打印输出
        sourceStream.print();
        // 提交执行
        env.execute();
    }

}
