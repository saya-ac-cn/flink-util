package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Title: Test_KeyedState
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 19:35
 * @Description:
 */

public class Test_KeyedState{

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

        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id").map(new DiyKeyCountMapper());

        resultStream.print();

        env.execute();

    }

}

class DiyKeyCountMapper extends RichMapFunction<SensorReading,Integer> {

    private ValueState<Integer> keyCount = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        keyCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
    }

    @Override
    public Integer map(SensorReading sensorReading) throws Exception {
        Integer count = keyCount.value();
        count++;
        keyCount.update(count);
        return count;
    }
}