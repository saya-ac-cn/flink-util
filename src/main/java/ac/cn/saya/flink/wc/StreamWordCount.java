package ac.cn.saya.flink.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 实时的字符统计计算
 * @Title: StreamWordCount
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/28 14:54
 * @Description:
 */

public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/Users/saya/project/java/flink-util/src/main/resources/wc-sample.txt";
        // 从文件中读取数据
        // DataStream<String> dataSource = env.readTextFile(inputPath);
        // 通过socket实时监听数据流 nc -lk 9000
        DataStream<String> dataSource = env.socketTextStream("127.0.0.1",9000);
        DataStream<Tuple2<String, Integer>> dataResult = dataSource.flatMap(new WordCountFlatMap())
                // 按照元组中第一个位置进行分组
                .keyBy(0)
                // 把元组中第2个位置的数做相加操作
                .sum(1);
        dataResult.print();

        // 执行任务
        env.execute();
    }

}
