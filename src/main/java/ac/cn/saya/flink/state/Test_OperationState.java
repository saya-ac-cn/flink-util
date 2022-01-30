package ac.cn.saya.flink.state;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Title: Test_OperationState
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/30 19:35
 * @Description:
 */

public class Test_OperationState{

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

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id").map(new DiyCountMapper());

        resultStream.print();

        env.execute();

    }

}

class DiyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {

    /**
     * 定义一个本地变量，最为原子状态
     */
    private Integer count = 0;

    @Override
    public List<Integer> snapshotState(long l, long l1) throws Exception {
        return Collections.singletonList(count);
    }

    @Override
    public void restoreState(List<Integer> list) throws Exception {
        for (Integer item:list) {
            count += item;
        }
    }

    @Override
    public Integer map(SensorReading sensorReading) throws Exception {
        count = count + 1;
        return count;
    }
}