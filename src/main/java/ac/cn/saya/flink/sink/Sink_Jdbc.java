package ac.cn.saya.flink.sink;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Title: Sink_Jdbc
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 23:17
 * @Description:
 */

public class Sink_Jdbc{

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://localhost:3306/flink").withDriverName("com.mysql.jdbc.Driver").withUsername("root").withPassword("123456").build();
        // 通过sink写入到mysql
        dataStream.addSink(JdbcSink.sink("replace into sensor_temp(`id`,`temp`) valuse (?,?)",(preparedStatement, sensorReading) -> {
            preparedStatement.setDouble(1,sensorReading.getTemperature());
            preparedStatement.setString(2,sensorReading.getId());
        },jdbcConnectionOptions));

        // 提交执行
        env.execute();
    }

}