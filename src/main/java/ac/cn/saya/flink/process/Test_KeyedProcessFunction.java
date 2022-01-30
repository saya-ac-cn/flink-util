package ac.cn.saya.flink.process;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Title: Test_KeyedProcessFunction
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 21:15
 * @Description:
 */

public class Test_KeyedProcessFunction{
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从socket中读取
        DataStream<String> sourceStream = env.socketTextStream("127.0.0.1",9000);        // 将文本数据清洗成对象数据
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });

        dataStream.keyBy("id").process(new DiyProcess()).print();

        env.execute();
    }
}

class DiyProcess extends KeyedProcessFunction<Tuple,SensorReading,Integer>{
    @Override
    public void processElement(SensorReading value, Context ctx, Collector<Integer> collector) throws Exception {
        collector.collect(value.getId().length());
        // context
        ctx.timestamp();
        ctx.getCurrentKey();
        //ctx.output();
        ctx.timerService().currentProcessingTime();
        ctx.timerService().currentWatermark();
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime());
        ctx.timerService().registerEventTimeTimer((value.getTimestamp()+10)*1000);
        ctx.timerService().deleteProcessingTimeTimer(10000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {

    }
}