package ac.cn.saya.flink.process;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Title: Test_ApplicationCase
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 21:15
 * @Description:
 */

public class Test_ApplicationCase{
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

        dataStream.keyBy("id").process(new TempConsIncreWarring(10)).print();

        env.execute();
    }
}

/**
 * 在 连续 interval 秒内，温度持续上升，将触发报警
 */
class TempConsIncreWarring extends KeyedProcessFunction<Tuple,SensorReading,String>{

    /**
     * 时间阈值
     */
    private Integer interval;

    public TempConsIncreWarring(Integer interval) {
        this.interval = interval;
    }

    /**
     * 定义状态， 保存上一次的温度值
     */
    private ValueState<Double> lastTempState;

    /**
     * 定义状态， 保存上一次的时间戳
     */
    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-time",Double.class,Double.MIN_VALUE));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
    }

    @Override
    public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
        // 取出状态
        Double lastTemp = lastTempState.value();
        Long timerTs = timerTsState.value();
        // 如果温度上升并且没有定时器，那么就注册10秒后的定时器，开始等待
        if (sensorReading.getTemperature() > lastTemp && null == timerTs){
            // 计算出定时器时间戳
            Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
            context.timerService().registerProcessingTimeTimer(ts);
            // 更新状态
            timerTsState.update(ts);
        }
        // 如果温度下降，那么删除定时器
        else if(sensorReading.getTemperature() < lastTemp && null != timerTs){
            context.timerService().deleteProcessingTimeTimer(timerTs);
            timerTsState.clear();
        }

        // 更新温度状态
        lastTempState.update(sensorReading.getTemperature());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发，输出报警信息
        out.collect("传感器"+ctx.getCurrentKey().getField(0)+"温度值连续"+interval+"s上升");
        timerTsState.clear();
    }

    @Override
    public void close() throws Exception {
        lastTempState.clear();
    }
}