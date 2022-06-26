package ac.cn.saya.flink.table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: Test_Common
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/31 16:56
 * @Description:
 */

public class Test_Common{

//    public static void main(String[] args) throws Exception {
//        // 1、创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // 2、创建表环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        String path = "/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt";
//        tableEnv.connect(new FileSystem().path(path))
//                .withFormat(new Csv())
//                .withSchema(new Schema().field("id", DataTypes.STRING())
//                .field("timestamp",DataTypes.BIGINT())
//                .field("temperature",DataTypes.DOUBLE())
//                ).createTemporaryTable("inputTable");
//
//        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print("init-data");
//
//        // 3、查询转换
//        // 3.1 table api
//        // 简单转换
//        Table resultTable = inputTable.select("id,temperature").filter("id === 'sensor_1'");
//
//        // 聚合统计
//        Table avgTable = inputTable.groupBy("id").select("id,id.count as count,temperature.avg as avgTemp");
//        Table sqlTable = tableEnv.sqlQuery("select id,temperature from inputTable where id = 'sensor_1'");
//        Table avgSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avgTemp from inputTable group by id");
//
//        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
//        tableEnv.toRetractStream(avgTable, Row.class).print("avgTable");
//        tableEnv.toAppendStream(sqlTable, Row.class).print("sqlTable");
//        tableEnv.toRetractStream(avgSqlTable, Row.class).print("avgSqlTable");
//
//        env.execute();
//    }

}