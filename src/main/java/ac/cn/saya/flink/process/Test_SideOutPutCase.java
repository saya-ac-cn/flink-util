package ac.cn.saya.flink.process;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Title: Test_SideOutPutCase
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 21:15
 * @Description:
 */

public class Test_SideOutPutCase{
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

        // 定义一个OutPutTag，用来表示侧输出低温流
        OutputTag<SensorReading> lowTag = new OutputTag<SensorReading>("low-temperature"){};

        // 测试processFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> hightTemperature = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                // 判断温度，大于30度，高温输出到主流，小于30度（低温流）输出到侧输出流
                if (sensorReading.getTemperature() > 30){
                    collector.collect(sensorReading);
                }else {
                    context.output(lowTag,sensorReading);
                }
            }
        });

        hightTemperature.print("high-temperature");
        hightTemperature.getSideOutput(lowTag).print("low-temperature");

        env.execute();
    }
}