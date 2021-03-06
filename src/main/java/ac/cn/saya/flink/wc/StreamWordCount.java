package ac.cn.saya.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
        // 1、创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2、通过socket实时监听数据流 nc -lk 9000
        DataStream<String> dataSource = env.socketTextStream("127.0.0.1",9000);
        // 3. 转换数据格式
        DataStream<Tuple2<String, Integer>> wordAndOneTuple = dataSource.flatMap(new WordCountFlatMap()).returns(Types.TUPLE(Types.STRING, Types.INT));
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