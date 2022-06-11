package ac.cn.saya.flink.window;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 统计网站的访问量
 *
 * @author saya
 * @title UrlCountViewExample
 * @datetime 2022年 06月 11日 4:49 下午
 * @version: 1.0
 * @description: TODO
 * PV 不排除重复的点击和用户数
 * UV 访问的用户数
 *
 */
public class UrlCountViewExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimeStamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("data");

        // 统计每个url的访问量
        stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountAgg(),new UrlViewCountResult())
                .print("total");


        env.execute();
    }

    // 增量聚合来一条数据+1
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            // 初始化
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulate) {
            return accumulate+1;
        }

        @Override
        public Long getResult(Long accumulate) {
            return accumulate;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义实现ProcessWindowFunction，包装窗口信息输出
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, Tuple4<String,Long,Long,Long>,String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, Tuple4<String,Long,Long,Long>, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<Tuple4<String,Long,Long,Long>> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = iterable.iterator().next();
            collector.collect(Tuple4.of(url,count,start,end));
        }
    }
}
