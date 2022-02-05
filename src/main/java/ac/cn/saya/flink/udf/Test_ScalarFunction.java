package ac.cn.saya.flink.udf;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @Title: Test_ScalarFunction
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/2/1 19:45
 * @Description:
 */

public class Test_ScalarFunction {

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

        HashCode hashCode = new HashCode(33);
        // 需要在环境中注册UDF
        tableEnv.registerFunction("hashCode",hashCode);

        Table resultTable = sensorTable.select("id,ts,temperature,hashCode(id) as code");

        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,temperature,hashCode(id) as code from sensor");

        tableEnv.toAppendStream(resultTable, Row.class).print("orm");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();

    }

    // 实现自定义的scalarFunction
    public static class HashCode extends ScalarFunction{

        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String val){
            return val.hashCode()*factor;
        }
    }

}
