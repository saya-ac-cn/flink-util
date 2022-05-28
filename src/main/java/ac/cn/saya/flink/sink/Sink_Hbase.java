package ac.cn.saya.flink.sink;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 写入到hbase
 *
 * @Title: Sink_Hbase
 * @ProjectName flink-util
 * @Author saya
 * @Date: 2022/5/28 22:56
 * @Description: TODO
 * 由于官方还没有提供直接写入到HBASE的接口方法，在这里通过RichSinkFunction自定义实现写入
 */

public class Sink_Hbase {

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
                return new SensorReading(fileds[0], Long.parseLong(fileds[1]), Double.parseDouble(fileds[2]));
            }
        });
        dataStream.addSink(new DiyHbaseMapper());

        // 提交执行
        env.execute();
    }
}

class DiyHbaseMapper extends RichSinkFunction<SensorReading> {

    // 管理 Hbase 的配置信息,这里因为 Configuration 的重名问题，将类以完整路径导入
    public org.apache.hadoop.conf.Configuration configuration;
    // 管理 Hbase 连接
    public Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop92:2181,hadoop93:2181,hadoop94:2181");
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        Table table = connection.getTable(TableName.valueOf("sensor_temp"));
        // 指定rowkey
        Put put = new Put("rowkey".getBytes((StandardCharsets.UTF_8)));
        // 指定列族并写入数据
        put.addColumn("info".getBytes(StandardCharsets.UTF_8), value.id.getBytes(StandardCharsets.UTF_8), value.temperature.toString().getBytes(StandardCharsets.UTF_8));
        // 执行put操作
        table.put(put);
        // 将表关闭
        table.close();
    }
}