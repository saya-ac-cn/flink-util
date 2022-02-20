package ac.cn.saya.flink.udf;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @Title: Test_AggregateFunction
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/2/1 19:45
 * @Description:
 */

public class Test_AggregateFunction {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");

        // 将文本数据清洗成对象数据
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });

        // 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

        AvgTemperature avgTemp = new AvgTemperature();
        // 需要在环境中注册UDF
        tableEnv.registerFunction("avgTemp",avgTemp);
        Table resultTable = sensorTable.groupBy("id").aggregate("avgTemp(temperature) as avgTemp").select("id,avgTemp");


        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, avgTemp(temperature) from sensor group by id");

        tableEnv.toRetractStream(resultTable, Row.class).print("orm");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();

    }

    // 实现自定义的AggregateFunction
    public static class AvgTemperature extends AggregateFunction<Double, Tuple2<Double,Integer>> {
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0d,0);
        }

        // 必须实现一个accumulator方法，求数据的更新状态
        public void accumulate(Tuple2<Double,Integer> accumulator,Double temperature){
            accumulator.f0 += temperature;
            accumulator.f1 += 1;
        }
    }

}
