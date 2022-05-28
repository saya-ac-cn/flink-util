package ac.cn.saya.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

public class Transform_Base {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");

        // 转换操作
        // map-> 计算长度
        DataStream<Integer> mapStream = sourceStream.map(
                (MapFunction<String, Integer>) String::length
        );

        // 使用lamba表达式可能因为类型擦除识别不了返回类型，所以需要手动指定
        //DataStream<Integer> mapStream = sourceStream.map(date -> date.length()).returns(Integer.class)


        // flatMap
//        DataStream<String> flatMapStream = sourceStream.flatMap((FlatMapFunction<String, String>) (val, collector) -> {
//            // 对本行的数据进行一次按，切割
//            String[] fields = val.split(",");
//            for (String field : fields) {
//                collector.collect(field);
//            }
//        });
        DataStream<String> flatMapStream = sourceStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] fields = value.split(",");
            for( String field: fields ){
                out.collect(field);
            }
        }).returns(String.class);

        // filter过滤(使用lamb表达式可以不用写返回类型，因为就不存在类型的转换，只是过滤判断)
        DataStream<String> filterStream = sourceStream.filter((FilterFunction<String>) (val)-> val.startsWith("sensor_1"));

        // 打印输出
        sourceStream.print("init");
        mapStream.print("map");
        flatMapStream.print("flat");
        filterStream.print("filter");
        // 提交执行
        env.execute();
    }

}
