package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author saya
 * @title: FakeWindowExample
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/21 22:05
 * @description:
 */

public class FakeWindowExample {

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
      stream.print("input");

      stream.keyBy(data -> data.url)
              // 用到了定时器，联想到process
              .process(new FakeWindowResult(1000))
              .print("total");


      env.execute();
   }

   static class FakeWindowResult extends KeyedProcessFunction<String, Event,String>{

      /**
       * 窗口大小
       */
      private long windowSize;

      /**
       * 定义一个mapstate，用来保存每个窗口中统计的count值
       */
      private MapState<Long,Long> windowUrlCountMapState;

      public FakeWindowResult(long windowSize) {
         this.windowSize = windowSize;
      }

      @Override
      public void open(Configuration parameters) throws Exception {
         windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
      }

      @Override
      public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
         // 每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
         long windowStart = value.timestamp / windowSize * windowSize;
         long windowEnd = windowStart + windowSize;
         // 注册一个end-1的定时器
         context.timerService().registerEventTimeTimer(windowEnd - 1);
         // 更新状态，进行增量聚合
         if (windowUrlCountMapState.contains(windowStart)){
            Long count = windowUrlCountMapState.get(windowStart);
            windowUrlCountMapState.put(windowStart,count+1);
         }else {
            windowUrlCountMapState.put(windowStart,1L);
         }
      }

      @Override
      public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
         long windowEnd = timestamp + 1;
         long windowStart = windowEnd - windowSize;
         Long count = windowUrlCountMapState.get(windowStart);
         out.collect("窗口[" + new Timestamp(windowStart) + "," + new Timestamp(windowEnd) + ")url="+ctx.getCurrentKey()+"的访问量为：" + count);
         // 模拟窗口的关闭，清除map对应的key-value
         windowUrlCountMapState.remove(windowStart);
      }
   }

}
