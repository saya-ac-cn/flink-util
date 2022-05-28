package ac.cn.saya.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过流的方式，处理批数据
 * @Title: StreamWordCount
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/28 14:54
 * @Description:
 */

public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/Users/saya/project/java/flink-util/src/main/resources/wc-sample.txt";
        // 2.从文件中读取数据
        DataStreamSource<String> dataSource = env.readTextFile(inputPath);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneTuple = dataSource.flatMap(new WordCountFlatMap()).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 4. 分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneTuple.keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOneKS.sum(1);
        // 6. 打印
        result.print();
        // 7. 执行
        env.execute();
    }

}
