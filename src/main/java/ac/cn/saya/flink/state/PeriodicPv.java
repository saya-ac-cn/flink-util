package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import ac.cn.saya.flink.window.UvCountExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author saya
 * @title: UvCount
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/19 17:17
 * @description:
 */

public class PeriodicPv {

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

      // 统计每个用户的点击事件，每隔10s滚动打印一次
      stream.keyBy(data -> data.user)
              .process(new PeriodicResult())
              .print("total");


      env.execute();
   }

    // 自定义实现KeyedProcessFunction
   static class PeriodicResult extends KeyedProcessFunction<String,Event,String> {
       ValueState<Long> countState;
       ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，就更新对应的count值
            Long count = countState.value();
            countState.update(null== count ? 1 : count + 1);
            // 如果没有注册定时器的话，就注册一个定时器
            if (null == timerState.value()) {
                context.timerService().registerEventTimeTimer(event.timestamp + 10000L);
                timerState.update(event.timestamp + 10000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发时，输出统计结果
            out.collect(ctx.getCurrentKey()+" 's pv count:"+countState.value());
            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp+10000L);
            timerState.update(timestamp+10000L);
        }
    }
}


