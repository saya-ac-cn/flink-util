package ac.cn.saya.flink.window;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 使用自定义实现的AggregateFunction和自定义实现的ProcessWindowFunction 完成UV
 *
 * @author saya
 * @title UvCountExample
 * @datetime 2022年 06月 11日 4:49 下午
 * @version: 1.0
 * @description: TODO
 * PV 不排除重复的点击和用户数
 * UV 访问的用户数
 *
 */
public class UvCountExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimeStamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("data");

        // 所有数据放在一起
        // 放到一个分区
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new UvAgg(),new UvResult())
                .print("total");


        env.execute();
    }

    // 自定义实现AggregateFunction，增量聚合计算uv值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            // 初始化
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulate) {
            accumulate.add(value.user);
            return accumulate;
        }

        @Override
        public Long getResult(HashSet<String> accumulate) {
            // 窗口触发计算，输出pv和uv的比值
            return (long) accumulate.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> longHashSetTuple2, HashSet<String> acc1) {
            return null;
        }
    }

    // 自定义实现ProcessWindowFunction，包装窗口信息输出
    public static class UvResult extends ProcessWindowFunction<Long,String,Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口["+new Timestamp(start)+"，"+new Timestamp(end)+")的uv值为："+iterable.iterator().next());
        }
    }
}
