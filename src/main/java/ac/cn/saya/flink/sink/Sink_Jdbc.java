package ac.cn.saya.flink.sink;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

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

        // Redis 连接池
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
        // 通过sink写入到redis
        dataStream.addSink(new DiyJdbcMapper());

        // 提交执行
        env.execute();
    }

}

class DiyJdbcMapper extends RichSinkFunction<SensorReading> {

    Connection conn = null;
    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        // 调用连接执行sql
        updateStmt.setDouble(1,value.getTemperature());
        updateStmt.setString(2, value.getId());
        updateStmt.execute();
        // 如果刚刚执行修改的影响行数为0，那么说明数据没有，需要执行一次添加操作
        if (updateStmt.getUpdateCount() == 0){
            insertStmt.setString(1, value.getId());
            insertStmt.setDouble(2,value.getTemperature());
            insertStmt.execute();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 用于建立数据库库的连接
        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test","root","123456");
        // 创建SQL预编译
        insertStmt = conn.prepareStatement("insert into sensor_temp(`id`,`temp`) valuse (?,?)");
        updateStmt = conn.prepareStatement("update sensor_temp set `temp` = ? where `id` = ?");
    }

    @Override
    public void close() throws Exception {
        // 释放连接资源
        insertStmt.close();
        updateStmt.close();
        conn.close();
    }
}