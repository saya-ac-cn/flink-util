package ac.cn.saya.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Title: Test_KafkaPipeline
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/31 20:54
 * @Description:
 */

public class Test_KafkaPipeline {
//    public static void main(String[] args) throws Exception {
//        // 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.connect(new Kafka().version("0.11")
//                        .topic("sensor")
//                        .property("zookeeper.connect", "127.0.0.1:2181")
//                        .property("bootstrap.servers", "127.0.0.1:9092"))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temperature", DataTypes.DOUBLE()))
//                .createTemporaryTable("inputTable");
//
//        Table sensorTable = tableEnv.from("inputTable");
//        Table resultTable = sensorTable.select("id,temperature").filter("id === 'sensor_1'");
//
//        Table avgTable = sensorTable.groupBy("id").select("id,id.count as count,temperature as avgTemp");
//        tableEnv.connect(new Kafka().version("0.11")
//                        .topic("sink-test")
//                        .property("zookeeper.connect", "127.0.0.1:2181")
//                        .property("bootstrap.servers", "127.0.0.1:9092"))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temperature", DataTypes.DOUBLE()))
//                .createTemporaryTable("outputTable");
//
//        resultTable.insertInto("outputTable");
//
//        env.execute();
//    }
}
