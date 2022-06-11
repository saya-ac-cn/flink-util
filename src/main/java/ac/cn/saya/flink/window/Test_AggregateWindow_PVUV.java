package ac.cn.saya.flink.window;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.time.Duration;
import java.util.HashSet;

/**
 * 开窗统计pv和uv，两者相除得到 平均用户活跃度
 *
 * @author saya
 * @title Test_CountWindow_PVUV
 * @datetime 2022年 06月 11日 4:49 下午
 * @version: 1.0
 * @description: TODO
 * PV 不排除重复的点击和用户数
 * UV 访问的用户数
 *
 * data> Event{user='Alice', url='./prod?id=1', timestamp=2022-06-11 18:04:23.386}
 * data> Event{user='Mary', url='./prod?id=1', timestamp=2022-06-11 18:04:24.393}
 * total> 1.0 [18:04:14,18:04:24) 1/1
 * data> Event{user='Alice', url='./prod?id=2', timestamp=2022-06-11 18:04:25.396}
 * data> Event{user='Bob', url='./cart', timestamp=2022-06-11 18:04:26.397}
 * data> Event{user='Cary', url='./prod?id=2', timestamp=2022-06-11 18:04:27.4}
 * total> 1.5 [18:04:16,18:04:26) 3/2
 * data> Event{user='Mary', url='./home', timestamp=2022-06-11 18:04:28.405}
 * data> Event{user='Alice', url='./prod?id=1', timestamp=2022-06-11 18:04:29.41}
 * total> 1.25 [18:04:18,18:04:28) 5/4
 */
public class Test_AggregateWindow_PVUV {

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
                .aggregate(new AvgPv())
                .print("total");


        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            // 初始化
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulate) {
            // 每来一条数据，pv个数加1，将user放入hashset
            accumulate.f1.add(value.user);
            return Tuple2.of(accumulate.f0 + 1, accumulate.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulate) {
            // 窗口触发计算，输出pv和uv的比值
            return (double) accumulate.f0 / accumulate.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
