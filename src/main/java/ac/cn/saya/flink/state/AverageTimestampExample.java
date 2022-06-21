package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author saya
 * @title: AverageTimestampExample
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/21 22:41
 * @description:
 */

public class AverageTimestampExample {

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

      stream.keyBy(data -> data.user).flatMap(new AvgResult(5L)).print("result");

      env.execute();

   }


   static class AvgResult extends RichFlatMapFunction<Event,String>{

      private long count;

      public AvgResult(long count) {
         this.count = count;
      }

      private AggregatingState<Event,Long> avgTsAggState;

      private ValueState<Long> countState;

      @Override
      public void open(Configuration parameters) throws Exception {
         avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long,Long>,Long>("avg-ts",
                 new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                       return Tuple2.of(0L,0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                       return Tuple2.of(accumulator.f0+value.timestamp,accumulator.f1+1);
                    }

                    @Override
                    public Long getResult(Tuple2<Long, Long> accumulator) {
                       return accumulator.f0/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                       return null;
                    }
                 }
                 , Types.TUPLE(Types.LONG, Types.LONG)));
         countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
      }

      @Override
      public void flatMap(Event event, Collector<String> collector) throws Exception {
         // 每来一条数据，当前的count加1
         Long currentCount = countState.value();
         if (null == currentCount){
            currentCount = 1L;
         }else {
            currentCount++;
         }
         // 更新状态
         countState.update(currentCount);
         avgTsAggState.add(event);
         // 如果达到count的阈值，则输出结果
         if (currentCount >= count){
            collector.collect(event.user+"过去"+count+"次访问平均时间戳为："+avgTsAggState.get());
            // 清理状态
            countState.clear();
            //avgTsAggState.clear();
         }
      }
   }
}
