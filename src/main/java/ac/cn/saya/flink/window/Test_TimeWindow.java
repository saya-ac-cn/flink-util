package ac.cn.saya.flink.window;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Title: Test_TimeWindow
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 10:26
 * @Description:
 */

public class Test_TimeWindow{
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从socket中读取
        DataStream<String> sourceStream = env.socketTextStream("127.0.0.1",9000);

        // 将文本数据清洗成对象数据
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });

        // 时间窗口函数
        DataStream<Integer> timeStream = dataStream.keyBy("id")
                //.countWindow(10,2)
                //.window(EventTimeSessionWindows.withGap(Time.minutes(1)))
                .timeWindow(Time.seconds(15))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        // 初始值
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer total) {
                        return total + 1;
                    }

                    @Override
                    public Integer getResult(Integer total) {
                        return total;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        // 全窗口函数
        DataStream<Tuple3<String, Long, Integer>> globalStream = dataStream.keyBy("id").timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        int count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, timeWindow.getEnd(), count));
                    }
                });

        // 打印输出
        timeStream.print("timeStream");
        globalStream.print("globalStream");

        // 提交执行
        env.execute();
    }
}
