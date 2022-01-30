package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @Title: Test_KeyedStateApplicationCase
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 19:35
 * @Description:
 */

public class Test_KeyedStateApplicationCase{

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

        // 定义一个flatmap，检测温度跳变，输出报警
        DataStream<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id").flatMap(new TempChangeWarring(10d));

        env.execute();

    }

}

class TempChangeWarring extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

    /**
     * 私有属性温度变化阈值
     */
    private Double threshold;

    public TempChangeWarring(Double threshold) {
        this.threshold = threshold;
    }
    private ValueState<Double> lastTempState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tempState",Double.class));
    }

    @Override
    public void close() throws Exception {
        lastTempState.clear();
    }

    @Override
    public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
        // 获取上次的温度值
        Double lastTemp = lastTempState.value();
        // 如果上一次的状态不为空，那么就判断两次的温度差值
        if (null != lastTemp){
            double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
            if (diff >= threshold){
                collector.collect(new Tuple3<>(sensorReading.getId(),lastTemp, sensorReading.getTemperature()));
            }
        }
        // 更新本次状态
        lastTempState.update(sensorReading.getTemperature());
    }
}