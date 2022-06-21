package ac.cn.saya.flink.process;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 统计近10秒内的网站访问，每隔5秒刷新一次（滑动窗口）
 *
 * @author saya
 * @title: Test_ProcessFunction_TopN
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/12 20:27
 * @description:
 */

public class Test_ProcessFunction_TopN {

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

        // 直接开窗，收集所有数据排序
        stream.map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();

        env.execute();

    }

    // 自定义增量聚合
    static class UrlHashMapCountAgg implements AggregateFunction<String, Map<String, Long>, List<Tuple2<String, Long>>> {
        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>(16);
        }

        @Override
        public Map<String, Long> add(String value, Map<String, Long> accumulator) {
            if (accumulator.containsKey(value)) {
                accumulator.put(value, accumulator.get(value) + 1);
            } else {
                accumulator.put(value, 1L);
            }
            return accumulator;
        }

        @Override
        public List<Tuple2<String, Long>> getResult(Map<String, Long> accumulator) {
            List<Tuple2<String, Long>> result = new ArrayList<>();
            for (Map.Entry<String, Long> item : accumulator.entrySet()) {
                result.add(Tuple2.of(item.getKey(), item.getValue()));
            }
            result.sort((o1, o2) -> o2.f1.compareTo(o1.f1));
            return result;
        }

        @Override
        public Map<String, Long> merge(Map<String, Long> stringLongMap, Map<String, Long> acc1) {
            return null;
        }
    }

    static class UrlAllWindowResult extends ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<List<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            List<Tuple2<String, Long>> list = iterable.iterator().next();
            StringBuilder result = new StringBuilder();
            result.append("-------------------------\n");
            result.append("窗口结束时间：").append(new Timestamp(context.window().getEnd())).append("\n");
            // 取出list的前2个，包装信息输出
            for (int i = 0; i < Math.min(2, list.size()); i++) {
                Tuple2<String, Long> currentTuple = list.get(i);
                result.append("No.").append(i + 1).append("，url:").append(currentTuple.f0).append("，访问量：").append(currentTuple.f1).append("\n");
            }
            result.append("-------------------------\n");
            collector.collect(result.toString());
        }
    }

}