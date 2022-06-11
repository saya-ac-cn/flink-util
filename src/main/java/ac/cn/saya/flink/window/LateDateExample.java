package ac.cn.saya.flink.window;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.entity.SensorReading;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 迟到数据处理
 *
 * @author saya
 * @title LateDateExample
 * @datetime 2022年 06月 11日 4:49 下午
 * @version: 1.0
 * @description: TODO
 */
public class LateDateExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        // 从socket中读取
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("127.0.0.1", 9000).map(new MapFunction<String, Event>() {
            @Override
            public Event map(String line) throws Exception {
                String[] fileds = line.split(" ");
                return new Event(fileds[0].trim(), fileds[1].trim(), Long.parseLong(fileds[2].trim()));
            }
        })
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimeStamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("data");

        // 定义一个输出标签
        OutputTag<Event> tag = new OutputTag<Event>("late-data"){};

        // 统计每个url的访问量
        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(tag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        result.print("total");
        result.getSideOutput(tag).print("late");

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
