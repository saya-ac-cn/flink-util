package ac.cn.saya.flink.table;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Title: Example
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/31 16:04
 * @Description:
 */

public class Example{
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2、从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");
        // 3、转换成实体实体
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });
        // 4、创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 5、基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 6、调用table api进行转换操作
        Table resultTable = dataTable.select("id,temperature").where("id = 'sensor_1'");

        // 通过sql进行查询
        // 注册表
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = "select id,temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        // 7、打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("orm");
        tableEnv.toAppendStream(resultSqlTable,Row.class).print("sql");

        // 8、提交执行
        env.execute();
    }
}
